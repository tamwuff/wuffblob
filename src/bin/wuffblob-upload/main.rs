// The uploader is written in such a way that the first error it runs into,
// it will stop. Any progress it has made so far will remain in the cloud,
// obviously, and if the upload is retried later it will mean that much less
// work that needs to be retried. But we won't purposely press on after an
// error. We want the user to be aware, and fix it.
//
// Here is how the queues work. Implicitly every stage can add something to
// the error queue and cause the whole thing to halt. That is not explicitly
// specified in the below, but it is implied everywhere.
//   - The lister thread generates tuples of (local_path, remote_path,
//     metadata) and feeds them into the metadata queue.
//   - The metadata task always does a "get" in Azure. If it determines that
//     all we need to do is make an empty directory, it'll take care of that
//     itself, rather than farming that off to some other task. This is why
//     it has to have a more inclusive name than the "getter task". It feeds
//     into either the uploader queue or the local-hasher queue.
//   - The local hasher thread feeds into the uploader queue.
//   - The uploader task is also responsible for deleting the old blob in
//     Azure before it starts the upload, if that is needed. Yes that is a
//     metadata operation, yes it would be slightly more efficient to let
//     the metadata task handle that part, but that would introduce a cycle
//     in the graph and it's just easier to keep things linear. After it is
//     done uploading, if the user has requested verification, it feeds into
//     the verification queue.
//
// The error queue should only be used for errors that relate directly to
// files on the local filesystem or in the cloud. It shouldn't be used for
// "failed to write to queue" types of things. Just panic in that case.
// The main thread will collect everyone's panics and all the errors, and
// it will decide how to present all of it to the user.

mod ctx;
mod state_machine;

fn list_local_dirs(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    err_writer: tokio::sync::mpsc::Sender<String>,
    metadata_writer: tokio::sync::mpsc::Sender<
        Box<crate::state_machine::Uploader>,
    >,
) {
    // The rule is: you don't add stuff to the LIFO unless you have verified
    // that it is actually a bona-fide directory, and not just a symlink that
    // points at a directory.
    //
    // Exception: the very first entries we are seeded with from the ctx, may
    // be symlinks that point at directories.
    let mut lifo: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(ctx.to_upload.len());
    let dispatch = |uploader: Box<crate::state_machine::Uploader>| {
        println!("{:#?}", uploader.as_ref());
    };

    for (local_path, remote_path) in &ctx.to_upload {
        let maybe_metadata: Result<std::fs::Metadata, std::io::Error> =
            std::fs::metadata(local_path);
        if let Err(e) = maybe_metadata {
            err_writer
                .blocking_send(format!("{:?}: {}", local_path, e))
                .expect("error reader died");
            continue;
        }
        let metadata: std::fs::Metadata = maybe_metadata.unwrap();
        let file_type: std::fs::FileType = metadata.file_type();
        if file_type.is_file() {
            dispatch(Box::new(crate::state_machine::Uploader::new(
                &ctx,
                local_path.clone(),
                remote_path.clone(),
                metadata,
            )));
        } else if file_type.is_dir() {
            lifo.push((local_path.clone(), remote_path.clone()));
        } else {
            err_writer
                .blocking_send(format!(
                    "{:?}: cannot upload {:?}",
                    local_path, file_type
                ))
                .expect("error reader died");
        }
    }

    while let Some((local_path, remote_path)) = lifo.pop() {
        let maybe_dir_entries: Result<std::fs::ReadDir, std::io::Error> =
            std::fs::read_dir(&local_path);
        if let Err(e) = maybe_dir_entries {
            err_writer
                .blocking_send(format!("{:?}: {}", local_path, e))
                .expect("error reader died");
            continue;
        }
        let dir_entries: std::fs::ReadDir = maybe_dir_entries.unwrap();
        let mut seen_any: bool = false;
        for maybe_dir_entry in dir_entries {
            seen_any = true;
            if let Err(e) = maybe_dir_entry {
                err_writer
                    .blocking_send(format!("{:?}: {}", local_path, e))
                    .expect("error reader died");
                continue;
            }
            let dir_entry: std::fs::DirEntry = maybe_dir_entry.unwrap();
            let new_basename: std::ffi::OsString = dir_entry.file_name();
            let new_local_path: std::path::PathBuf =
                local_path.join(&new_basename);
            let mut new_remote_path: wuffblob::path::WuffPath =
                remote_path.clone();
            new_remote_path.push(&new_basename);
            let maybe_orig_file_type: Result<
                std::fs::FileType,
                std::io::Error,
            > = dir_entry.file_type();
            if let Err(e) = maybe_orig_file_type {
                err_writer
                    .blocking_send(format!("{:?}: {}", new_local_path, e))
                    .expect("error reader died");
                continue;
            }
            let orig_file_type: std::fs::FileType =
                maybe_orig_file_type.unwrap();
            let maybe_new_metadata: Result<std::fs::Metadata, std::io::Error> =
                std::fs::metadata(&new_local_path);
            if let Err(e) = maybe_new_metadata {
                err_writer
                    .blocking_send(format!("{:?}: {}", new_local_path, e))
                    .expect("error reader died");
                continue;
            }
            let new_metadata: std::fs::Metadata = maybe_new_metadata.unwrap();
            // We mostly trust the metadata as far as what kind of thing this
            // is. But we do check one thing, which is we definitely want to
            // kick it out if it's a symlink that is pointing to a directory.
            let file_type: std::fs::FileType = new_metadata.file_type();
            if file_type.is_file() {
                dispatch(Box::new(crate::state_machine::Uploader::new(
                    &ctx,
                    new_local_path,
                    new_remote_path,
                    new_metadata,
                )));
            } else if file_type.is_dir() {
                if orig_file_type.is_symlink() {
                    err_writer
                        .blocking_send(format!(
                            "{:?}: cannot upload {:?}",
                            new_local_path, orig_file_type
                        ))
                        .expect("error reader died");
                } else {
                    lifo.push((new_local_path, new_remote_path));
                }
            } else {
                err_writer
                    .blocking_send(format!(
                        "{:?}: cannot upload {:?}",
                        new_local_path, file_type
                    ))
                    .expect("error reader died");
            }
        }
        if !seen_any {
            let maybe_metadata: Result<std::fs::Metadata, std::io::Error> =
                std::fs::metadata(&local_path);
            if let Err(e) = maybe_metadata {
                err_writer
                    .blocking_send(format!("{:?}: {}", local_path, e))
                    .expect("error reader died");
                continue;
            }
            let metadata: std::fs::Metadata = maybe_metadata.unwrap();
            // It had better be a directory...!
            let file_type: std::fs::FileType = metadata.file_type();
            if file_type.is_dir() {
                dispatch(Box::new(crate::state_machine::Uploader::new(
                    &ctx,
                    local_path,
                    remote_path,
                    metadata,
                )));
            } else {
                err_writer
                    .blocking_send(format!(
                        "{:?}: expecting directory, found {:?}",
                        local_path, file_type
                    ))
                    .expect("error reader died");
            }
        }
    }

    //metadata_writer.blocking_send(uploader).expect("Metadata fetcher task died");
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

    // I would use a tokio::sync::oneshot::channel for errors, but its Sender
    // doesn't implement Clone... So the err channel is an MPSC channel, but
    // it is used as if it were a one-shot.
    let (err_writer, err_reader) =
        tokio::sync::mpsc::channel::<String>(1000usize);
    let (metadata_writer, metadata_reader) = tokio::sync::mpsc::channel::<
        Box<crate::state_machine::Uploader>,
    >(1000usize);
    let (hasher_writer, hasher_reader) = tokio::sync::mpsc::channel::<
        Box<crate::state_machine::Uploader>,
    >(1000usize);
    let (uploader_writer, uploader_reader) = tokio::sync::mpsc::channel::<
        Box<crate::state_machine::Uploader>,
    >(1000usize);
    let (verifier_writer, verifier_reader) = tokio::sync::mpsc::channel::<
        Box<crate::state_machine::Uploader>,
    >(1000usize);

    let lister_thread: tokio::task::JoinHandle<()> =
        ctx.base_ctx.get_async_spawner().spawn_blocking({
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            let err_writer: tokio::sync::mpsc::Sender<String> =
                err_writer.clone();
            move || {
                list_local_dirs(ctx, err_writer, metadata_writer);
            }
        });
    lister_thread.await;

    Ok(())
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command =
        wuffblob::ctx::make_cmdline_parser("wuffblob-upload")
            .arg(
                clap::Arg::new("paths")
                    .value_parser(clap::value_parser!(std::path::PathBuf))
                    .action(clap::ArgAction::Append),
            )
            .arg(
                clap::Arg::new("upload_as")
                    .long("upload-as")
                    .value_parser(clap::value_parser!(
                        wuffblob::path::WuffPath
                    ))
                    .action(clap::ArgAction::Set),
            )
            .arg(
                clap::Arg::new("force")
                    .long("force")
                    .short('f')
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                clap::Arg::new("verify")
                    .long("verify")
                    .action(clap::ArgAction::SetTrue),
            );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let raw_to_upload: Vec<&std::path::PathBuf> = if let Some(paths) =
        cmdline_matches.get_many::<std::path::PathBuf>("paths")
    {
        Vec::from_iter(paths)
    } else {
        Vec::new()
    };
    let mut to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(raw_to_upload.len());
    if let Some(raw_upload_as) =
        cmdline_matches.get_one::<wuffblob::path::WuffPath>("upload_as")
    {
        if let Some(upload_as) =
            raw_upload_as.clone().canonicalize_or_componentless()
        {
            if raw_to_upload.is_empty() {
                to_upload.push((".".into(), upload_as));
            } else if raw_to_upload.len() == 1 {
                to_upload.push((raw_to_upload[0].clone(), upload_as));
            } else {
                return Err("If upload-as is specified, there must be at most one local path specified to upload".into());
            }
        } else {
            return Err(format!(
                "Upload-as path given as {} which is not canonicalizable",
                raw_upload_as
            )
            .into());
        }
    } else if raw_to_upload.is_empty() {
        to_upload.push((".".into(), wuffblob::path::WuffPath::new()));
    } else {
        for path in raw_to_upload {
            if let Some(upload_as) =
                TryInto::<wuffblob::path::WuffPath>::try_into(path.as_path())?
                    .canonicalize_or_componentless()
            {
                to_upload.push((path.clone(), upload_as));
            } else {
                return Err(format!(
                    "{:?} is not canonicalizable (try --upload-as)",
                    path
                )
                .into());
            }
        }

        // make sure there's no overlap
        let mut paths: Vec<&wuffblob::path::WuffPath> =
            Vec::with_capacity(to_upload.len());
        for (_, path) in &to_upload {
            paths.push(path);
        }
        wuffblob::path::WuffPath::check_for_overlap(paths)?;
    }

    let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::new(
        crate::ctx::Ctx::new(&cmdline_matches, to_upload)?,
    );
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
