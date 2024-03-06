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

async fn async_main(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
) -> Result<(), wuffblob::error::WuffError> {
    ctx.base_ctx.install_siginfo_handler({
        let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
        move || {
            crate::ctx::siginfo_handler(&ctx);
        }
    })?;

    println!("TO UPLOAD: {:#?}", &ctx.to_upload);

    Ok(())
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command = wuffblob::ctx::make_cmdline_parser("wuffblob-upload")
        .arg(
            clap::Arg::new("paths")
                .value_parser(clap::value_parser!(std::path::PathBuf))
                .action(clap::ArgAction::Append),
        )
        .arg(
            clap::Arg::new("upload_as")
                .long("upload-as")
                .value_parser(clap::value_parser!(wuffblob::path::WuffPath))
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
                .short('f')
                .action(clap::ArgAction::SetTrue),
        );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let raw_to_upload: Vec<&std::path::PathBuf> =
        if let Some(paths) = cmdline_matches.get_many::<std::path::PathBuf>("paths") {
            Vec::from_iter(paths)
        } else {
            Vec::new()
        };
    let mut to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(raw_to_upload.len());
    if let Some(raw_upload_as) = cmdline_matches.get_one::<wuffblob::path::WuffPath>("upload_as") {
        if let Some(upload_as) = raw_upload_as.clone().canonicalize_or_componentless() {
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
            if let Some(upload_as) = TryInto::<wuffblob::path::WuffPath>::try_into(path.as_path())?
                .canonicalize_or_componentless()
            {
                to_upload.push((path.clone(), upload_as));
            } else {
                return Err(format!("{:?} is not canonicalizable (try --upload-as)", path).into());
            }
        }

        // make sure there's no overlap
        let mut paths: Vec<&wuffblob::path::WuffPath> = Vec::with_capacity(to_upload.len());
        for (_, path) in &to_upload {
            paths.push(path);
        }
        wuffblob::path::WuffPath::check_for_overlap(paths)?;
    }

    let ctx: std::sync::Arc<crate::ctx::Ctx> =
        std::sync::Arc::new(crate::ctx::Ctx::new(&cmdline_matches, to_upload)?);
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
