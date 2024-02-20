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
//
// The way the queues work:
//   - The main thread feeds things into either the UI queue or the hasher
//     queue
//   - The hasher task feeds things into the UI queue
//   - That means everything is now going into a single stream into the UI
//     queue
//   - The UI thread feeds things into the properties updater queue
//   - The properties updater task might feed some things back into the UI
//     queue, but only for error reporting purposes. There's no chance of
//     a repeating cycle.
//   - Once the properties updater task is done (and the UI thread is done
//     with reporting any errors) we're done.

mod ctx;
mod state_machine;

async fn do_hash(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    file_checker: &mut crate::state_machine::FileChecker,
) {
    if let crate::state_machine::FileCheckerState::Hash = file_checker.state {
    } else {
        panic!(
            "State is {:?}, expected FileCheckerState::Hash",
            &file_checker.state
        );
    }

    let maybe_filename_as_string: Result<String, std::ffi::OsString> =
        file_checker.path.to_osstring().into_string();
    if maybe_filename_as_string.is_err() {
        file_checker.hash_failed(
            ctx,
            &wuffblob::error::WuffError::from("path is not valid unicode"),
        );
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.hash_required -= 1u64;
            stats.hash_bytes_required -= file_checker.properties.content_length;
        });
        return;
    }
    let blob_client = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(maybe_filename_as_string.unwrap());
    let mut chunks_stream = blob_client.get().into_stream();
    let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
    let expected_len: u64 = file_checker.properties.content_length;
    let mut num_bytes_hashed: u64 = 0u64;
    // We desperately do not want to move anything here, because moves
    // mean copying. We borrow even when it would seem to be in our best
    // interest to consume...!
    while let Some(ref mut maybe_chunk) = futures::stream::StreamExt::next(&mut chunks_stream).await
    {
        match maybe_chunk {
            Ok(ref mut chunk) => {
                let chunk_stream = &mut chunk.data;
                while let Some(ref maybe_buf) = futures::stream::StreamExt::next(chunk_stream).await
                {
                    match maybe_buf {
                        Ok(ref buf) => {
                            let buf_len: usize = buf.len();
                            <md5::Md5 as md5::Digest>::update(&mut hasher, buf);
                            num_bytes_hashed += buf_len as u64;
                            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                                stats.hash_bytes_complete += buf_len as u64;
                            });
                        }
                        Err(err) => {
                            file_checker.hash_failed(ctx, &wuffblob::error::WuffError::from(err));
                            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                                stats.hash_bytes_complete -= num_bytes_hashed;
                                stats.hash_required -= 1u64;
                                stats.hash_bytes_required -= expected_len;
                            });
                            return;
                        }
                    }
                }
            }
            Err(ref err) => {
                file_checker.hash_failed(ctx, &wuffblob::error::WuffError::from(err));
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.hash_bytes_complete -= num_bytes_hashed;
                    stats.hash_required -= 1u64;
                    stats.hash_bytes_required -= expected_len;
                });
                return;
            }
        }
    }
    if num_bytes_hashed != expected_len {
        file_checker.hash_failed(
            ctx,
            &wuffblob::error::WuffError::from(format!(
                "Expected {} bytes, got {} instead",
                expected_len, num_bytes_hashed
            )),
        );
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.hash_bytes_complete -= num_bytes_hashed;
            stats.hash_required -= 1u64;
            stats.hash_bytes_required -= expected_len;
        });
        return;
    }
    file_checker.provide_hash(
        ctx,
        <md5::Md5 as md5::Digest>::finalize(hasher)
            .as_slice()
            .try_into()
            .unwrap(),
    );
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.hash_complete += 1;
    });
}

async fn hasher(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut hasher_reader: tokio::sync::mpsc::Receiver<Box<crate::state_machine::FileChecker>>,
    ui_writer: tokio::sync::mpsc::Sender<Option<Box<crate::state_machine::FileChecker>>>,
) -> Result<(), wuffblob::error::WuffError> {
    let conc_mgr = ctx
        .base_ctx
        .data_concurrency_mgr::<Box<crate::state_machine::FileChecker>>();
    while let Some(mut file_checker) = hasher_reader.recv().await {
        if let crate::state_machine::FileCheckerState::Hash = file_checker.state {
        } else {
            return Err(format!(
                "State is {:?}, expected FileCheckerState::Hash",
                &file_checker.state
            )
            .into());
        }
        let fut = {
            let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
            async move {
                do_hash(&ctx, file_checker.as_mut()).await;
                file_checker
            }
        };
        for maybe_file_checker in conc_mgr.spawn(&ctx.base_ctx, fut).await {
            let file_checker: Box<crate::state_machine::FileChecker> = maybe_file_checker?;
            match file_checker.state {
                crate::state_machine::FileCheckerState::Terminal => {}
                crate::state_machine::FileCheckerState::Message(_)
                | crate::state_machine::FileCheckerState::UserInteraction(_) => {
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
        let file_checker: Box<crate::state_machine::FileChecker> = maybe_file_checker?;
        match file_checker.state {
            crate::state_machine::FileCheckerState::Terminal => {}
            crate::state_machine::FileCheckerState::Message(_)
            | crate::state_machine::FileCheckerState::UserInteraction(_) => {
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
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut ui_reader: tokio::sync::mpsc::Receiver<Option<Box<crate::state_machine::FileChecker>>>,
    propupd_writer: tokio::sync::mpsc::Sender<Box<crate::state_machine::FileChecker>>,
) {
    // Turn our properties-update writer into an Option so that we can drop
    // it when we need to
    let mut maybe_propupd_writer: Option<
        tokio::sync::mpsc::Sender<Box<crate::state_machine::FileChecker>>,
    > = Some(propupd_writer);
    while let Some(maybe_file_checker) = ui_reader.blocking_recv() {
        if let Some(mut file_checker) = maybe_file_checker {
            let name: std::ffi::OsString = file_checker.path.to_osstring();
            loop {
                match file_checker.state {
                    crate::state_machine::FileCheckerState::Terminal => {
                        break;
                    }
                    crate::state_machine::FileCheckerState::Message(ref msg) => {
                        println!("{:?}: {}", name, msg);
                        file_checker.message_printed();
                    }
                    crate::state_machine::FileCheckerState::UserInteraction(
                        ref proposed_repair,
                    ) => {
                        print!("{:?}: {}", name, proposed_repair.problem_statement);
                        if ctx.base_ctx.dry_run {
                            print!("\n{}? no\n\n", proposed_repair.question);
                            file_checker.provide_user_input(&ctx, false);
                        } else if ctx.preen {
                            println!(" ({})", proposed_repair.action);
                            file_checker.provide_user_input(&ctx, true);
                        } else if ctx.yes {
                            print!("\n{}? yes\n\n", proposed_repair.question);
                            file_checker.provide_user_input(&ctx, true);
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
                            file_checker.provide_user_input(&ctx, answer);
                        }
                    }
                    crate::state_machine::FileCheckerState::UpdateProperties => {
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

async fn do_update_properties(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    file_checker: &mut crate::state_machine::FileChecker,
) {
    if let crate::state_machine::FileCheckerState::UpdateProperties = file_checker.state {
    } else {
        panic!(
            "State is {:?}, expected FileCheckerState::UpdateProperties",
            &file_checker.state
        );
    }
    assert!(file_checker.properties_dirty);
    let maybe_filename_as_string: Result<String, std::ffi::OsString> =
        file_checker.path.to_osstring().into_string();
    if maybe_filename_as_string.is_err() {
        file_checker.update_properties_failed(
            ctx,
            &wuffblob::error::WuffError::from("path is not valid unicode"),
        );
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.propupd_required -= 1;
        });
        return;
    }
    let blob_client = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(maybe_filename_as_string.unwrap());
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.any_propupd_attempted = true;
    });
    match blob_client
        .set_properties()
        .set_from_blob_properties(file_checker.properties.clone())
        .into_future()
        .await
    {
        Ok(_) => {
            file_checker.properties_updated();
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.propupd_complete += 1;
            });
        }
        Err(err) => {
            file_checker.update_properties_failed(ctx, &wuffblob::error::WuffError::from(err));
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.propupd_required -= 1;
            });
        }
    }
}

async fn properties_updater(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut propupd_reader: tokio::sync::mpsc::Receiver<Box<crate::state_machine::FileChecker>>,
    ui_writer: tokio::sync::mpsc::Sender<Option<Box<crate::state_machine::FileChecker>>>,
) -> Result<(), wuffblob::error::WuffError> {
    let conc_mgr = ctx
        .base_ctx
        .metadata_concurrency_mgr::<Box<crate::state_machine::FileChecker>>();
    while let Some(mut file_checker) = propupd_reader.recv().await {
        if let crate::state_machine::FileCheckerState::UpdateProperties = file_checker.state {
        } else {
            return Err(format!(
                "State is {:?}, expected FileCheckerState::UpdateProperties",
                &file_checker.state
            )
            .into());
        }

        let fut = {
            let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
            async move {
                do_update_properties(&ctx, file_checker.as_mut()).await;
                file_checker
            }
        };
        for maybe_file_checker in conc_mgr.spawn(&ctx.base_ctx, fut).await {
            let file_checker: Box<crate::state_machine::FileChecker> = maybe_file_checker?;
            match file_checker.state {
                crate::state_machine::FileCheckerState::Terminal => {}

                crate::state_machine::FileCheckerState::Message(_) => {
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
        let file_checker: Box<crate::state_machine::FileChecker> = maybe_file_checker?;
        match file_checker.state {
            crate::state_machine::FileCheckerState::Terminal => {}

            crate::state_machine::FileCheckerState::Message(_) => {
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
    ctx: std::sync::Arc<crate::ctx::Ctx>,
) -> Result<(), wuffblob::error::WuffError> {
    ctx.base_ctx.install_siginfo_handler({
        let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
        move || {
            crate::ctx::siginfo_handler(&ctx);
        }
    })?;

    let (ui_writer, ui_reader) =
        tokio::sync::mpsc::channel::<Option<Box<crate::state_machine::FileChecker>>>(1000usize);
    let (hasher_writer, hasher_reader) =
        tokio::sync::mpsc::channel::<Box<crate::state_machine::FileChecker>>(1000usize);
    let (propupd_writer, propupd_reader) =
        tokio::sync::mpsc::channel::<Box<crate::state_machine::FileChecker>>(1000usize);

    let ui_thread: tokio::task::JoinHandle<()> = ctx.base_ctx.get_async_spawner().spawn_blocking({
        let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
        move || {
            ui(ctx, ui_reader, propupd_writer);
        }
    });
    let hasher_task: tokio::task::JoinHandle<Result<(), wuffblob::error::WuffError>> =
        ctx.base_ctx.get_async_spawner().spawn(hasher(
            std::sync::Arc::clone(&ctx),
            hasher_reader,
            ui_writer.clone(),
        ));
    let propupd_task: tokio::task::JoinHandle<Result<(), wuffblob::error::WuffError>> =
        ctx.base_ctx.get_async_spawner().spawn(properties_updater(
            std::sync::Arc::clone(&ctx),
            propupd_reader,
            ui_writer.clone(),
        ));

    let mut contents = ctx
        .base_ctx
        .azure_client
        .container_client
        .list_blobs()
        .into_stream();
    while let Some(possible_chunk) = futures::stream::StreamExt::next(&mut contents).await {
        // We want to consume 'possible_chunk'. Normally we would be very
        // careful when iterating, *not* to consume what we are iterating
        // over. Here we are equally careful *to* consume it.
        let chunk: azure_storage_blobs::container::operations::list_blobs::ListBlobsResponse =
            possible_chunk?;

        // chunk.blobs.blobs() is a nice convenience function but it only
        // returns refs... we want to consume...
        for blob_item in chunk.blobs.items {
            if let azure_storage_blobs::container::operations::BlobItem::Blob(blob) = blob_item {
                let mut is_dir: bool = false;
                if let Some(ref resource_type) = blob.properties.resource_type {
                    if resource_type == "directory" {
                        is_dir = true;
                    }
                }
                if is_dir {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.dirs_found += 1u64;
                    });
                } else {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.files_found += 1u64;
                        stats.bytes_found += blob.properties.content_length;
                    });
                }

                // Here's the big payoff for us having consumed everything:
                // we get to move blob.properties. Yay!
                let file_checker: Box<crate::state_machine::FileChecker> =
                    Box::new(crate::state_machine::FileChecker::new(
                        &ctx,
                        wuffblob::path::WuffPath::from_osstr(std::ffi::OsStr::new(&blob.name)),
                        is_dir,
                        blob.properties,
                    ));

                match file_checker.state {
                    crate::state_machine::FileCheckerState::Terminal => {}

                    crate::state_machine::FileCheckerState::Message(_)
                    | crate::state_machine::FileCheckerState::UserInteraction(_) => {
                        ui_writer.send(Some(file_checker)).await?;
                    }

                    crate::state_machine::FileCheckerState::Hash => {
                        hasher_writer.send(file_checker).await?;
                    }

                    _ => {
                        return Err(format!(
                            "unexpected file checker state {:?} in main thread",
                            &file_checker.state
                        )
                        .into());
                    }
                }
            }
        }
    }
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.done_listing = true;
    });

    drop(hasher_writer);
    hasher_task.await??;

    ui_writer.send(None).await?;
    drop(ui_writer);

    propupd_task.await??;

    ui_thread.await?;

    let stats: crate::ctx::Stats = ctx.get_stats();
    println!(
        "{} files, {} bytes used",
        stats.files_found, stats.bytes_found
    );
    if stats.any_propupd_attempted && !ctx.preen {
        print!("\n***** FILE SYSTEM WAS MODIFIED *****\n");
    }
    if stats.any_not_repaired {
        println!("");
        Err("***** SOME FILES WERE NOT REPAIRED *****".into())
    } else {
        Ok(())
    }
}

fn main() -> Result<(), wuffblob::error::WuffError> {
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

    let ctx: std::sync::Arc<crate::ctx::Ctx> =
        std::sync::Arc::new(crate::ctx::Ctx::new(&cmdline_matches)?);
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
