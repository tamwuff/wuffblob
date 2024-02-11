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
//   - etag (it should be the same as the hash, in hex format)
// Additionally, in non-preen mode, we will:
//   - check the hash even if it had an MD5 already
//
// We don't consider it our purview to examine or modify:
//   - content disposition
//   - cache control
//   - content language
//   - access tier
//
// I am using unbounded queues everywhere, primarily because a full run in
// non-preen mode could take hours, and I don't want the list-blobs operation
// to time out if we don't request the next page for hours. Mitigating factor:
// preen mode will only add things to the queue if there is something wrong
// with them, so preen mode should use way way less memory. Non-preen mode
// will be more of a memory hog and that's just life.

#[derive(Debug)]
struct FsckCtx {
    preen: bool,
    yes: bool,
}

#[derive(Debug, Clone)]
enum RepairType {
    ContentType,
    Md5,
    Etag,
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
    // I don't need to be put into any queue. This file (or directory) is good.
    Ok,

    // I don't need to be put into any queue. We can't fix it.
    NotRepairable(String),

    // I don't need to be put into any queue. We could have fixed it, but the
    // user told us not to.
    NotRepaired,

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
    any_repairs_declined: bool,
}

impl FileChecker {
    fn new(
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
        name: &str,
        is_dir: bool,
        properties: azure_storage_blobs::blob::BlobProperties,
    ) -> FileChecker {
        let path: wuffblob::wuffpath::WuffPath =
            wuffblob::wuffpath::WuffPath::from_osstr(std::ffi::OsStr::new(name));
        let desired_content_type: &'static str = ctx.get_desired_mime_type(&path);
        let mut checker: FileChecker = FileChecker {
            path: path,
            desired_content_type: desired_content_type,
            is_dir: is_dir,
            properties: properties,
            state: FileCheckerState::Ok,
            empirical_md5: None,
            possible_repairs: Vec::<ProposedRepair>::new(),
            authorized_repairs: Vec::<RepairType>::new(),
            any_repairs_declined: false,
        };
        if !checker.path.is_canonical() {
            checker.state =
                FileCheckerState::NotRepairable("path is not in canonical form".to_string());
        } else if checker.is_dir {
            // not much else to check!
        } else if checker.properties.content_md5.is_none() || !fsck_ctx.preen {
            checker.state = FileCheckerState::Hash;
        } else {
            checker.analyze(ctx, fsck_ctx);
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
                self.any_repairs_declined = true;
            }
            if let Some(next_possible_repair) = self.possible_repairs.pop() {
                self.state = FileCheckerState::UserInteraction(next_possible_repair);
            } else if !self.authorized_repairs.is_empty() {
                self.state = FileCheckerState::UpdateProperties;
            } else if self.any_repairs_declined {
                self.state = FileCheckerState::NotRepaired;
            } else {
                self.state = FileCheckerState::Ok;
            }
        }
    }

    async fn do_hash(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if let FileCheckerState::Hash = self.state {
        } else {
            return;
        }

        let maybe_filename_as_string: Result<String, std::ffi::OsString> =
            self.path.to_osstring().into_string();
        if maybe_filename_as_string.is_err() {
            self.state = FileCheckerState::NotRepairable("path is not valid unicode".to_string());
            return;
        }
        let blob_client = ctx
            .azure_client
            .container_client
            .blob_client(maybe_filename_as_string.unwrap());
        let mut chunks_stream = blob_client.get().into_stream();
        let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
        while let Some(maybe_chunk) = futures::stream::StreamExt::next(&mut chunks_stream).await {
            match maybe_chunk {
                Ok(chunk) => {
                    let mut chunk_stream = chunk.data;
                    while let Some(maybe_buf) =
                        futures::stream::StreamExt::next(&mut chunk_stream).await
                    {
                        match maybe_buf {
                            Ok(buf) => {
                                <md5::Md5 as md5::Digest>::update(&mut hasher, buf);
                            }
                            Err(err) => {
                                self.state = FileCheckerState::NotRepairable(format!("{:?}", err));
                                return;
                            }
                        }
                    }
                }
                Err(err) => {
                    self.state = FileCheckerState::NotRepairable(format!("{:?}", err));
                    return;
                }
            }
        }
        self.empirical_md5 = Some(
            <md5::Md5 as md5::Digest>::finalize(hasher)
                .as_slice()
                .try_into()
                .unwrap(),
        );
    }

    async fn do_update_properties(
        &mut self,
        ctx: &std::sync::Arc<wuffblob::ctx::Ctx>,
        fsck_ctx: &std::sync::Arc<FsckCtx>,
    ) {
        if let FileCheckerState::UpdateProperties = self.state {
        } else {
            return;
        }
        if self.authorized_repairs.is_empty() {
            return;
        }
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
                RepairType::Etag => {
                    if let Some(ref empirical_md5) = self.empirical_md5 {
                        self.properties.etag = wuffblob::ctx::hex_encode(empirical_md5).into();
                    } else {
                        self.properties.etag = wuffblob::ctx::hex_encode(
                            self.properties.content_md5.as_ref().unwrap().as_slice(),
                        )
                        .into();
                    }
                }
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
            if AsRef::<str>::as_ref(&self.properties.etag)
                != wuffblob::ctx::hex_encode(empirical_md5)
            {
                self.possible_repairs.push(ProposedRepair {
                    repair_type: RepairType::Etag,
                    problem_statement: "ETag does not match MD5 hash".to_string(),
                    question: "Update ETag",
                    action: "ETag updated",
                });
            }
            if let Some(ref md5_from_metadata) = self.properties.content_md5 {
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
        } else {
            let presumed_md5: &[u8; 16] = self.properties.content_md5.as_ref().unwrap().as_slice();
            if AsRef::<str>::as_ref(&self.properties.etag)
                != wuffblob::ctx::hex_encode(presumed_md5)
            {
                self.possible_repairs.push(ProposedRepair {
                    repair_type: RepairType::Etag,
                    problem_statement: "ETag does not match MD5 hash".to_string(),
                    question: "Update ETag",
                    action: "ETag updated",
                });
            }
        }
        if let Some(possible_repair) = self.possible_repairs.pop() {
            self.state = FileCheckerState::UserInteraction(possible_repair);
        } else {
            self.state = FileCheckerState::Ok;
        }
    }
}

async fn hasher(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    mut hasher_reader: tokio::sync::mpsc::UnboundedReceiver<FileChecker>,
    ui_writer: std::sync::mpsc::Sender<FileChecker>,
) {
    while let Some(mut file_checker) = hasher_reader.recv().await {
        if let FileCheckerState::Hash = &file_checker.state {
            file_checker.do_hash(&ctx, &fsck_ctx).await
        }
        match &file_checker.state {
            FileCheckerState::Ok | FileCheckerState::NotRepaired => {}

            FileCheckerState::NotRepairable(_) | FileCheckerState::UserInteraction(_) => {
                ui_writer.send(file_checker).expect("UI thread died");
            }

            _ => {
                panic!("unknown file checker state in hasher task");
            }
        }
    }
}

fn ui(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    ui_reader: std::sync::mpsc::Receiver<FileChecker>,
    propupd_writer: tokio::sync::mpsc::UnboundedSender<FileChecker>,
) {
    while let Ok(mut file_checker) = ui_reader.recv() {
        let name: std::ffi::OsString = file_checker.path.to_osstring();
        loop {
            match &file_checker.state {
                FileCheckerState::Ok | FileCheckerState::NotRepaired => {
                    break;
                }
                FileCheckerState::NotRepairable(msg) => {
                    println!("{:?}: {}", name, msg);
                    break;
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
                        print!("\n{}? [yn]", proposed_repair.question);
                        std::io::Write::flush(&mut std::io::stdout().lock()).expect("stdout");
                        let mut answer: bool = false;
                        loop {
                            let mut s = String::new();
                            if let Ok(_) =
                                std::io::BufRead::read_line(&mut std::io::stdin().lock(), &mut s)
                            {
                                s = String::from(s.trim());
                                if s.eq_ignore_ascii_case("y") {
                                    answer = true;
                                    break;
                                } else if s.eq_ignore_ascii_case("n") {
                                    answer = false;
                                    break;
                                }
                            } else {
                                return;
                            }
                        }
                        file_checker.user_input(&ctx, &fsck_ctx, answer);
                    }
                }
                FileCheckerState::UpdateProperties => {
                    propupd_writer
                        .send(file_checker)
                        .expect("Properties updater task died");
                    break;
                }
                _ => {
                    panic!("unknown file checker state in UI thread");
                }
            }
        }
    }
}

async fn properties_updater(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
    mut propupd_reader: tokio::sync::mpsc::UnboundedReceiver<FileChecker>,
) {
    while let Some(mut file_checker) = propupd_reader.recv().await {
        if let FileCheckerState::UpdateProperties = &file_checker.state {
            file_checker.do_update_properties(&ctx, &fsck_ctx).await
        }
        match &file_checker.state {
            FileCheckerState::Ok | FileCheckerState::NotRepaired => {}

            FileCheckerState::NotRepairable(msg) => {
                let name: std::ffi::OsString = file_checker.path.to_osstring();
                println!("{:?}: {}", name, msg);
            }

            _ => {
                panic!("unknown file checker state in properties updater task");
            }
        }
    }
}

async fn async_main(
    ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    fsck_ctx: std::sync::Arc<FsckCtx>,
) -> Result<(), wuffblob::error::WuffBlobError> {
    // Blocking MPSC channel for sending stuff to the UI thread
    let (ui_writer, ui_reader) = std::sync::mpsc::channel();

    // Async MPSC channels for sending stuff to the hasher and the properties
    // updater
    let (hasher_writer, hasher_reader) = tokio::sync::mpsc::unbounded_channel();
    let (propupd_writer, propupd_reader) = tokio::sync::mpsc::unbounded_channel();

    // Spawn the UI thread and the hasher task first. They should be safe to
    // run while we're listing the container's contents. I don't want to get
    // into doing any property updates until we've gotten a clean list of the
    // container.
    let ui_thread: tokio::task::JoinHandle<()> =
    {
        let ctx_for_ui: std::sync::Arc<wuffblob::ctx::Ctx> =
            std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx);
        let fsck_ctx_for_ui: std::sync::Arc<FsckCtx> = std::sync::Arc::<FsckCtx>::clone(&fsck_ctx);
            ctx.get_async_spawner().spawn_blocking(move || {
                ui(ctx_for_ui, fsck_ctx_for_ui, ui_reader, propupd_writer);
            })
    };
    let hasher_task: tokio::task::JoinHandle<()> = ctx.get_async_spawner().spawn(hasher(
        std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
        std::sync::Arc::<FsckCtx>::clone(&fsck_ctx),
        hasher_reader,
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
                let file_checker: FileChecker =
                    FileChecker::new(&ctx, &fsck_ctx, &blob.name, is_dir, blob.properties);
                match &file_checker.state {
                    FileCheckerState::Ok | FileCheckerState::NotRepaired => {}

                    FileCheckerState::NotRepairable(_) | FileCheckerState::UserInteraction(_) => {
                        ui_writer.send(file_checker).expect("UI thread died");
                    }

                    FileCheckerState::Hash => {
                        hasher_writer.send(file_checker).expect("Hasher task died");
                    }

                    _ => {
                        panic!("unknown file checker state in main thread");
                    }
                }
            }
        }
    }

    drop(ui_writer);
    drop(hasher_writer);

    let propupd_task: tokio::task::JoinHandle<()> =
        ctx.get_async_spawner().spawn(properties_updater(
            std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
            std::sync::Arc::<FsckCtx>::clone(&fsck_ctx),
            propupd_reader,
        ));

    ui_thread.await?;
    hasher_task.await?;
    propupd_task.await?;
    Ok(())
}

fn main() -> Result<(), wuffblob::error::WuffBlobError> {
    let cmdline_parser: clap::Command = wuffblob::ctx::make_cmdline_parser("wuffblob-fsck");
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let ctx: std::sync::Arc<wuffblob::ctx::Ctx> =
        std::sync::Arc::<wuffblob::ctx::Ctx>::new(wuffblob::ctx::Ctx::new(&cmdline_matches));
    let fsck_ctx: std::sync::Arc<FsckCtx> = std::sync::Arc::<FsckCtx>::new(FsckCtx {
        preen: false,
        yes: false,
    });
    ctx.run_async_main(async_main(
        std::sync::Arc::<wuffblob::ctx::Ctx>::clone(&ctx),
        fsck_ctx,
    ))
}
