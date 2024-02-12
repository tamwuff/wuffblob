// Useful reference for all the ergonomics of how an fsck-like thing ought
// to behave:
// https://cgit.freebsd.org/src/tree/sbin/fsck_ffs?id=771a16a90f448bb28f827ec47322471859cfb7aa (as of 1995)
// https://cgit.freebsd.org/src/diff/?id=6db798cae4e1313dd0ef9b2bcaceca5ee73c75fd (SIGINFO support)
//
// In our case, Azure seems to follow the same (or stricter) path
// canonicalization rules as we do, so we don't have to write code to
// *fix* non-canonical paths. We should check for them, but it's ok to
// just error out if we find any, we don't have to try to repair them.
//
// That also means we don't have to pay much attention to directories. The
// only thing we can really do with a directory is make sure its path is
// canonical...
//
// In our case, preen mode will mean to do as much as we can without user
// input and without taking *too* horribly long. Non-preen mode will download
// everything and check the MD5s, so it will take longer, but it will give
// extra confidence that the data is ok. In non-preen mode we will also ask
// permission before we do anything.
//
// In all modes, we will iterate through all blobs and check:
//   - content type (it should match what we think it should be)
//   - MD5 (it should have one; if it doesn't, start hashing in the background)
// Additionally, in non-preen mode, we will:
//   - check the hash even if it had an MD5 already
//
// Azure does not let us modify:
//   - etag
//
// We don't consider it our purview to examine or modify:
//   - content disposition
//   - cache control
//   - content language
//   - access tier
//
// Using unbounded queues is scary because infinite memory usage. But using
// bounded queues is scary too, because the whole thing is being driven by
// a recursive list operation and it might have some kind of session token
// in it that could time out if we don't ask for the next chunk for too long
// a period of time.
//
// I don't think there's really a choice. Unbounded isn't an option. So I
// am going to use bounded queues everywhere with a relatively small size
// (~= 1000 elements) and we will just have to hope that the recursive list
// operations don't time out.
//
// Something that should be a mitigating factor in preen mode, is that in
// preen mode we will only add things to a queue if there is something
// demonstrably wrong with them. So not too many things should even end up
// in a queue (or waiting to get into a queue) in the first place. And I guess
// for non-preen mode you just have to run it from a machine with a fast pipe
// to Azure, so that the hashing goes fast, and you probably have to pass -n
// or -y, so that the UI goes fast.

#[derive(Debug, Clone)]
struct FsckStats {
    files_found: u64,
    dirs_found: u64,
    done_listing: bool,
    any_not_repaired: bool,
    user_input_required: u64,
    user_input_complete: u64,
    hash_required: u64,
    hash_complete: u64,
    hash_bytes_required: u64,
    hash_bytes_complete: u64,
    propupd_required: u64,
    propupd_complete: u64,
}

impl FsckStats {
    fn new() -> FsckStats {
        FsckStats {
            files_found: 0u64,
            dirs_found: 0u64,
            done_listing: false,
            any_not_repaired: false,
            user_input_required: 0u64,
            user_input_complete: 0u64,
            hash_required: 0u64,
            hash_complete: 0u64,
            hash_bytes_required: 0u64,
            hash_bytes_complete: 0u64,
            propupd_required: 0u64,
            propupd_complete: 0u64,
        }
    }
}

#[derive(Debug)]
struct FsckCtx {
    preen: bool,
    yes: bool,
    stats: std::sync::Mutex<FsckStats>,
}

impl FsckCtx {
    fn get_stats(&self) -> FsckStats {
        self.stats.lock().expect("stats").clone()
    }

    fn mutate_stats<F>(&self, cb: F)
    where
        F: FnOnce(&mut FsckStats),
    {
        cb(&mut self.stats.lock().expect("stats"));
    }
}

#[derive(Debug, Clone)]
enum RepairType {
    ContentType,
    Md5,
}

#[derive(Debug, Clone)]
struct ProposedRepair {
    repair_type: RepairType,
    problem_statement: String,
    question: &'static str,
    action: &'static str,
}

#[derive(Debug, Clone)]
enum FileCheckerState {
    // I don't need to be put into any queue. Terminal state.
    Terminal,

    // Please put me into the user-interface queue. I don't need any response
    // from the user, but I would like a message to be printed.
    Message(String),

    // Please put me into the user-interface queue. We can fix this, but we
    // need to keep the user informed and/or ask them whether to go ahead.
    UserInteraction(ProposedRepair),

    // Please put me into the queue to be hashed.
    Hash,

    // Please put me into the queue for the final properties update.
    UpdateProperties,
}

#[derive(Debug)]
struct FileChecker {
    path: wuffblob::wuffpath::WuffPath,
    desired_content_type: &'static str,
    is_dir: bool,
    properties: azure_storage_blobs::blob::BlobProperties,
    state: FileCheckerState,
    empirical_md5: Option<[u8; 16]>,
    possible_repairs: Vec<ProposedRepair>,
    authorized_repairs: Vec<RepairType>,
}

impl FileChecker {
    fn new(
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
        path: wuffblob::wuffpath::WuffPath,
        is_dir: bool,
        properties: azure_storage_blobs::blob::BlobProperties,
    ) -> FileChecker {
        let mut checker: FileChecker = FileChecker {
            path: path,
            desired_content_type: "",
            is_dir: is_dir,
            properties: properties,
            state: FileCheckerState::Terminal,
            empirical_md5: None,
            possible_repairs: Vec::<ProposedRepair>::new(),
            authorized_repairs: Vec::<RepairType>::new(),
        };
        if !checker.path.is_canonical() {
            checker.state = FileCheckerState::Message("path is not in canonical form".to_string());
            fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                stats.any_not_repaired = true;
            });
        } else if checker.is_dir {
            // not much else to check!
        } else {
            checker.desired_content_type = ctx.get_desired_mime_type(&checker.path);
            if checker.properties.content_md5.is_none() || !fsck_ctx.preen {
                checker.state = FileCheckerState::Hash;
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.hash_required += 1u64;
                    stats.hash_bytes_required += checker.properties.content_length;
                });
            } else {
                checker.analyze(ctx, fsck_ctx);
            }
        }
        checker
    }

    fn user_input(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
        answer: bool,
    ) {
        if let FileCheckerState::UserInteraction(ref possible_repair) = self.state {
            if answer {
                self.authorized_repairs
                    .push(possible_repair.repair_type.clone());
            } else {
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.any_not_repaired = true;
                });
            }
            if let Some(next_possible_repair) = self.possible_repairs.pop() {
                self.state = FileCheckerState::UserInteraction(next_possible_repair);
            } else if !self.authorized_repairs.is_empty() {
                self.state = FileCheckerState::UpdateProperties;
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.user_input_complete += 1u64;
                    stats.propupd_required += 1u64;
                });
            } else {
                self.state = FileCheckerState::Terminal;
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.user_input_complete += 1u64;
                });
            }
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::UserInteraction",
                &self.state
            );
        }
    }

    fn message_printed(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if let FileCheckerState::Message(_) = &self.state {
            self.state = FileCheckerState::Terminal;
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Message",
                &self.state
            );
        }
    }

    async fn do_hash(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if let FileCheckerState::Hash = &self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::Hash",
                &self.state
            );
        }

        let maybe_filename_as_string: Result<String, std::ffi::OsString> =
            self.path.to_osstring().into_string();
        if maybe_filename_as_string.is_err() {
            self.state = FileCheckerState::Message("path is not valid unicode".to_string());
            fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                stats.any_not_repaired = true;
                stats.hash_required -= 1u64;
                stats.hash_bytes_required -= self.properties.content_length;
            });
            return;
        }
        let blob_client = ctx
            .azure_client
            .container_client
            .blob_client(maybe_filename_as_string.unwrap());
        let mut chunks_stream = blob_client.get().into_stream();
        let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
        let expected_len: u64 = self.properties.content_length;
        let mut num_bytes_hashed: u64 = 0u64;
        // We desperately do not want to move anything here, because moves
        // mean copying. We borrow even when it would seem to be in our best
        // interest to consume...!
        while let Some(ref mut maybe_chunk) =
            futures::stream::StreamExt::next(&mut chunks_stream).await
        {
            match maybe_chunk {
                Ok(ref mut chunk) => {
                    let chunk_stream = &mut chunk.data;
                    while let Some(ref maybe_buf) =
                        futures::stream::StreamExt::next(chunk_stream).await
                    {
                        match maybe_buf {
                            Ok(ref buf) => {
                                let buf_len: usize = buf.len();
                                <md5::Md5 as md5::Digest>::update(&mut hasher, buf);
                                num_bytes_hashed += buf_len as u64;
                                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                                    stats.hash_bytes_complete += buf_len as u64;
                                });
                            }
                            Err(err) => {
                                self.state = FileCheckerState::Message(format!("{:?}", err));
                                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                                    stats.any_not_repaired = true;
                                    stats.hash_bytes_complete -= num_bytes_hashed;
                                    stats.hash_required -= 1u64;
                                    stats.hash_bytes_required -= expected_len;
                                });
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    self.state = FileCheckerState::Message(format!("{:?}", err));
                    fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                        stats.any_not_repaired = true;
                        stats.hash_bytes_complete -= num_bytes_hashed;
                        stats.hash_required -= 1u64;
                        stats.hash_bytes_required -= expected_len;
                    });
                    return;
                }
            }
        }
        if num_bytes_hashed != expected_len {
            self.state = FileCheckerState::Message(format!(
                "Expected {} bytes, got {} instead",
                expected_len, num_bytes_hashed
            ));
            fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                stats.any_not_repaired = true;
                stats.hash_bytes_complete -= num_bytes_hashed;
                stats.hash_required -= 1u64;
                stats.hash_bytes_required -= expected_len;
            });
            return;
        }
        self.empirical_md5 = Some(
            <md5::Md5 as md5::Digest>::finalize(hasher)
                .as_slice()
                .try_into()
                .unwrap(),
        );
        fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
            stats.hash_complete += 1;
        });
        self.analyze(ctx, fsck_ctx);
    }

    async fn do_update_properties(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if let FileCheckerState::UpdateProperties = self.state {
        } else {
            panic!(
                "State is {:?}, expected FileCheckerState::UpdateProperties",
                &self.state
            );
        }
        if self.authorized_repairs.is_empty() {
            panic!("do_update_properties(): no repairs authorized");
        }
        let maybe_filename_as_string: Result<String, std::ffi::OsString> =
            self.path.to_osstring().into_string();
        if maybe_filename_as_string.is_err() {
            self.state = FileCheckerState::Message("path is not valid unicode".to_string());
            fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                stats.any_not_repaired = true;
                stats.propupd_required -= 1;
            });
            return;
        }
        let blob_client = ctx
            .azure_client
            .container_client
            .blob_client(maybe_filename_as_string.unwrap());
        for repair_type in &self.authorized_repairs {
            match repair_type {
                RepairType::ContentType => {
                    self.properties.content_type = self.desired_content_type.to_string();
                }
                RepairType::Md5 => {
                    self.properties.content_md5 = Some(
                        azure_storage::prelude::ConsistencyMD5::decode(base64::Engine::encode(
                            &base64::prelude::BASE64_STANDARD,
                            self.empirical_md5.as_ref().unwrap(),
                        ))
                        .unwrap(),
                    );
                }
            }
        }

        match blob_client
            .set_properties()
            .set_from_blob_properties(self.properties.clone())
            .into_future()
            .await
        {
            Ok(_) => {
                self.state = FileCheckerState::Terminal;
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.propupd_complete += 1;
                });
            }
            Err(err) => {
                self.state = FileCheckerState::Message(format!("{:?}", err));
                fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                    stats.any_not_repaired = true;
                    stats.propupd_required -= 1;
                });
                return;
            }
        }
    }

    fn analyze(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if self.properties.content_type != self.desired_content_type {
            self.possible_repairs.push(ProposedRepair {
                repair_type: RepairType::ContentType,
                problem_statement: format!(
                    "Content type is {} but it should be {}",
                    self.properties.content_type, self.desired_content_type
                ),
                question: "Adjust",
                action: "Adjusted",
            });
        }
        if let Some(ref empirical_md5) = self.empirical_md5 {
            if let Some(ref md5_from_metadata) = self.properties.content_md5 {
                if md5_from_metadata.as_slice() != empirical_md5 {
                    self.possible_repairs.push(ProposedRepair {
                        repair_type: RepairType::Md5,
                        problem_statement: format!(
                            "Metadata lists MD5 hash as {} but it is actually {}",
                            wuffblob::ctx::hex_encode(md5_from_metadata.as_slice()),
                            wuffblob::ctx::hex_encode(empirical_md5)
                        ),
                        question: "Update hash",
                        action: "Hash updated",
                    });
                }
            } else {
                self.possible_repairs.push(ProposedRepair {
                    repair_type: RepairType::Md5,
                    problem_statement: format!(
                        "Metadata does not list an MD5 hash. Correct hash is {}",
                        wuffblob::ctx::hex_encode(empirical_md5)
                    ),
                    question: "Update hash",
                    action: "Hash updated",
                });
            }
        }
        if let Some(possible_repair) = self.possible_repairs.pop() {
            self.state = FileCheckerState::UserInteraction(possible_repair);
            fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                stats.user_input_required += 1;
            });
        } else {
            self.state = FileCheckerState::Terminal;
        }
    }
}

async fn hasher(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    mut hasher_reader: tokio::sync::mpsc::Receiver<FileChecker>,
    ui_writer: tokio::sync::mpsc::Sender<Option<FileChecker>>,
) -> Result<(), wuffblob::error::WuffBlobError> {
    let conc_mgr = ctx.data_concurrency_mgr::<FileChecker>();
    while let Some(mut file_checker) = hasher_reader.recv().await {
        if let FileCheckerState::Hash = &file_checker.state {
        } else {
            return Err(format!(
                "State is {:?}, expected FileCheckerState::Hash",
                &file_checker.state
            )
            .into());
        }
        let ctx_for_task: std::sync::Arc<wuffblob::ctx::Ctx> =
            std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx);
        let fsck_ctx_for_task: std::sync::Arc<FsckCtx> =
            std::sync::Arc::<FsckCtx>::clone(&fsck_ctx);
        let fut = async move {
            file_checker
                .do_hash(&ctx_for_task, &fsck_ctx_for_task)
                .await;
            file_checker
        };
        for maybe_file_checker in conc_mgr.spawn(&ctx, fut).await {
            let file_checker: FileChecker = maybe_file_checker?;
            match &file_checker.state {
                FileCheckerState::Terminal => {}
                FileCheckerState::Message(_) | FileCheckerState::UserInteraction(_) => {
                    ui_writer.send(Some(file_checker)).await?;
                }

                _ => {
                    return Err(format!(
                        "unexpected file checker state {:?} in hasher task",
                        &file_checker.state
                    )
                    .into());
                }
            }
        }
    }
    for maybe_file_checker in conc_mgr.drain().await {
        let file_checker: FileChecker = maybe_file_checker?;
        match &file_checker.state {
            FileCheckerState::Terminal => {}
            FileCheckerState::Message(_) | FileCheckerState::UserInteraction(_) => {
                ui_writer.send(Some(file_checker)).await?;
            }

            _ => {
                return Err(format!(
                    "unexpected file checker state {:?} in hasher task",
                    &file_checker.state
                )
                .into());
            }
        }
    }
    Ok(())
}

// The UI thread takes Option<FileChecker> rather than FileChecker. Sending
// it a None means that the hasher is complete, and it should drop its
// writer so that the properties updater gets an EOF. The UI thread will
// stay running, but after that point all it is expecting to receive is
// plain Messages.
fn ui(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    mut ui_reader: tokio::sync::mpsc::Receiver<Option<FileChecker>>,
    propupd_writer: tokio::sync::mpsc::Sender<FileChecker>,
) {
    // Turn our properties-update writer into an Option so that we can drop
    // it when we need to
    let mut maybe_propupd_writer: Option<tokio::sync::mpsc::Sender<FileChecker>> =
        Some(propupd_writer);
    while let Some(maybe_file_checker) = ui_reader.blocking_recv() {
        if let Some(mut file_checker) = maybe_file_checker {
            let name: std::ffi::OsString = file_checker.path.to_osstring();
            loop {
                match &file_checker.state {
                    FileCheckerState::Terminal => {
                        break;
                    }
                    FileCheckerState::Message(msg) => {
                        println!("{:?}: {}", name, msg);
                        file_checker.message_printed(&ctx, &fsck_ctx);
                    }
                    FileCheckerState::UserInteraction(proposed_repair) => {
                        print!("{:?}: {}", name, proposed_repair.problem_statement);
                        if ctx.dry_run {
                            print!("\n{}? no\n\n", proposed_repair.question);
                            file_checker.user_input(&ctx, &fsck_ctx, false);
                        } else if fsck_ctx.preen {
                            println!(" ({})", proposed_repair.action);
                            file_checker.user_input(&ctx, &fsck_ctx, true);
                        } else if fsck_ctx.yes {
                            print!("\n{}? yes\n\n", proposed_repair.question);
                            file_checker.user_input(&ctx, &fsck_ctx, true);
                        } else {
                            print!("\n{}? [yn] ", proposed_repair.question);
                            std::io::Write::flush(&mut std::io::stdout().lock()).expect("stdout");
                            let answer: bool = loop {
                                let mut s = String::new();
                                if let Ok(_) = std::io::BufRead::read_line(
                                    &mut std::io::stdin().lock(),
                                    &mut s,
                                ) {
                                    s = String::from(s.trim());
                                    if s.eq_ignore_ascii_case("y") {
                                        break true;
                                    } else if s.eq_ignore_ascii_case("n") {
                                        break false;
                                    }
                                } else {
                                    return;
                                }
                            };
                            file_checker.user_input(&ctx, &fsck_ctx, answer);
                        }
                    }
                    FileCheckerState::UpdateProperties => {
                        maybe_propupd_writer
                            .as_ref()
                            .unwrap()
                            .blocking_send(file_checker)
                            .expect("Properties updater task died");
                        break;
                    }
                    _ => {
                        panic!(
                            "unexpected file checker state {:?} in UI thread",
                            &file_checker.state
                        );
                    }
                }
            }
        } else {
            maybe_propupd_writer = None;
        }
    }
}

async fn properties_updater(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    mut propupd_reader: tokio::sync::mpsc::Receiver<FileChecker>,
    ui_writer: tokio::sync::mpsc::Sender<Option<FileChecker>>,
) -> Result<(), wuffblob::error::WuffBlobError> {
    let conc_mgr = ctx.metadata_concurrency_mgr::<FileChecker>();
    while let Some(mut file_checker) = propupd_reader.recv().await {
        if let FileCheckerState::UpdateProperties = &file_checker.state {
        } else {
            return Err(format!(
                "State is {:?}, expected FileCheckerState::UpdateProperties",
                &file_checker.state
            )
            .into());
        }

        let ctx_for_task: std::sync::Arc<wuffblob::ctx::Ctx> =
            std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx);
        let fsck_ctx_for_task: std::sync::Arc<FsckCtx> =
            std::sync::Arc::<FsckCtx>::clone(&fsck_ctx);
        let fut = async move {
            file_checker
                .do_update_properties(&ctx_for_task, &fsck_ctx_for_task)
                .await;
            file_checker
        };
        for maybe_file_checker in conc_mgr.spawn(&ctx, fut).await {
            let file_checker: FileChecker = maybe_file_checker?;
            match &file_checker.state {
                FileCheckerState::Terminal => {}

                FileCheckerState::Message(msg) => {
                    ui_writer.send(Some(file_checker)).await?;
                }

                _ => {
                    return Err(format!(
                        "unexpected file checker state {:?} in properties updater task",
                        &file_checker.state
                    )
                    .into());
                }
            }
        }
    }
    for maybe_file_checker in conc_mgr.drain().await {
        let file_checker: FileChecker = maybe_file_checker?;
        match &file_checker.state {
            FileCheckerState::Terminal => {}

            FileCheckerState::Message(msg) => {
                ui_writer.send(Some(file_checker)).await?;
            }

            _ => {
                return Err(format!(
                    "unexpected file checker state {:?} in properties updater task",
                    &file_checker.state
                )
                .into());
            }
        }
    }
    Ok(())
}

async fn async_main(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
) -> Result<(), wuffblob::error::WuffBlobError> {
    {
        let fsck_ctx_for_siginfo: std::sync::Arc<FsckCtx> =
            std::sync::Arc::<FsckCtx>::clone(&fsck_ctx);
        ctx.install_siginfo_handler(move || {
            let stats: FsckStats = fsck_ctx_for_siginfo.get_stats();
            let mut s: String = String::new();
            s.push_str("\n\n");
            if stats.done_listing {
                s.push_str(&format!(
                    "{} files and {} directories (final count)\n",
                    stats.files_found, stats.dirs_found
                ));
            } else {
                s.push_str(&format!(
                    "{} files and {} directories found so far (list not yet complete)\n",
                    stats.files_found, stats.dirs_found
                ));
            }
            if stats.user_input_required > 0u64 {
                s.push_str(&format!(
                    "User input: {} of {}\n",
                    stats.user_input_complete, stats.user_input_required
                ));
            }
            if stats.hash_required > 0u64 {
                s.push_str(&format!(
                    "Hashing: {} of {} ({} of {} bytes)\n",
                    stats.hash_complete,
                    stats.hash_required,
                    stats.hash_bytes_complete,
                    stats.hash_bytes_required
                ));
            }
            if stats.propupd_required > 0u64 {
                s.push_str(&format!(
                    "Repairs: {} of {}\n",
                    stats.propupd_complete, stats.propupd_required
                ));
            }
            s.push_str("\n");
            print!("{}", s);
        })?;
    }

    let (ui_writer, ui_reader) = tokio::sync::mpsc::channel::<Option<FileChecker>>(1000usize);
    let (hasher_writer, hasher_reader) = tokio::sync::mpsc::channel::<FileChecker>(1000usize);
    let (propupd_writer, propupd_reader) = tokio::sync::mpsc::channel::<FileChecker>(1000usize);

    let ui_thread: tokio::task::JoinHandle<()> = {
        let ctx_for_ui: std::sync::Arc<wuffblob::ctx::Ctx> =
            std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx);
        let fsck_ctx_for_ui: std::sync::Arc<FsckCtx> = std::sync::Arc::<FsckCtx>::clone(&fsck_ctx);
        ctx.get_async_spawner().spawn_blocking(move || {
            ui(ctx_for_ui, fsck_ctx_for_ui, ui_reader, propupd_writer);
        })
    };
    let hasher_task = ctx.get_async_spawner().spawn(hasher(
        std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
        std::sync::Arc::<FsckCtx>::clone(&fsck_ctx),
        hasher_reader,
        ui_writer.clone(),
    ));
    let propupd_task = ctx.get_async_spawner().spawn(properties_updater(
        std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
        std::sync::Arc::<FsckCtx>::clone(&fsck_ctx),
        propupd_reader,
        ui_writer.clone(),
    ));

    let mut contents = ctx.azure_client.container_client.list_blobs().into_stream();
    while let Some(possible_chunk) = futures::stream::StreamExt::next(&mut contents).await {
        // We want to consume 'possible_chunk'. Normally we would be very
        // careful when iterating, *not* to consume what we are iterating
        // over. Here we are equally careful *to* consume it.
        let chunk: azure_storage_blobs::container::operations::list_blobs::ListBlobsResponse =
            possible_chunk?;
        // chunk.blobs.blobs() is a nice convenience function but it only
        // returns refs...
        for blob_item in chunk.blobs.items {
            if let azure_storage_blobs::container::operations::BlobItem::Blob(blob) = blob_item {
                let mut is_dir: bool = false;
                if let Some(ref resource_type) = blob.properties.resource_type {
                    if resource_type == "directory" {
                        is_dir = true;
                    }
                }
                if is_dir {
                    fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                        stats.dirs_found += 1u64;
                    });
                } else {
                    fsck_ctx.mutate_stats(|stats: &mut FsckStats| {
                        stats.files_found += 1u64;
                    });
                }
                let file_checker: FileChecker = FileChecker::new(
                    &ctx,
                    &fsck_ctx,
                    wuffblob::wuffpath::WuffPath::from_osstr(std::ffi::OsStr::new(&blob.name)),
                    is_dir,
                    blob.properties,
                );
                match file_checker.state {
                    FileCheckerState::Terminal => {}

                    FileCheckerState::Message(_) | FileCheckerState::UserInteraction(_) => {
                        ui_writer.send(Some(file_checker)).await?;
                    }

                    FileCheckerState::Hash => {
                        hasher_writer.send(file_checker).await?;
                    }

                    _ => {
                        panic!(
                            "unexpected file checker state {:?} in main thread",
                            &file_checker.state
                        );
                    }
                }
            }
        }
    }

    drop(hasher_writer);
    hasher_task.await??;

    ui_writer.send(None).await?;
    drop(ui_writer);

    propupd_task.await??;

    ui_thread.await?;

    let stats: FsckStats = fsck_ctx.get_stats();
    if stats.any_not_repaired {
        Err("Some files were not repaired.".into())
    } else {
        Ok(())
    }
}

fn main() -> Result<(), wuffblob::error::WuffBlobError> {
    let cmdline_parser: clap::Command = wuffblob::ctx::make_cmdline_parser("wuffblob-fsck")
        .arg(
            clap::Arg::new("preen")
                .long("preen")
                .short('p')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            clap::Arg::new("yes")
                .long("yes")
                .short('y')
                .action(clap::ArgAction::SetTrue),
        );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let ctx: std::sync::Arc<wuffblob::ctx::Ctx> =
        std::sync::Arc::<wuffblob::ctx::Ctx>::new(wuffblob::ctx::Ctx::new(&cmdline_matches)?);
    let fsck_ctx: std::sync::Arc<FsckCtx> = std::sync::Arc::<FsckCtx>::new(FsckCtx {
        preen: *(cmdline_matches.get_one::<bool>("preen").unwrap()),
        yes: *(cmdline_matches.get_one::<bool>("yes").unwrap()),
        stats: std::sync::Mutex::<FsckStats>::new(FsckStats::new()),
    });
    ctx.run_async_main(async_main(
        std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
        fsck_ctx,
    ))
}
