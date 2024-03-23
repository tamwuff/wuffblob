// The downloader is written in such a way that the first error it runs into,
// it will stop. Any progress it has made so far will remain on the local disk,
// obviously, and if the download is retried later it will mean that much less
// work that needs to be retried. But we won't purposely press on after an
// error. We want the user to be aware, and fix it.
//
//   - The feeder is responsible for figuring out the remote_path, the
//     local_path, and the remote_metadata, then it passes it on to the
//     mkdir stage.
//   - The mkdir stage does two jobs: it makes directories that correspond
//     to directories in Azure; and it makes parent directories for things
//     that are files in Azure. For directories, they're done after that.
//     For files, they get passed on to the open stage.
//   - The open stage is simple: it just opens the file, for both reading
//     and writing. It might pass things on to the local "prefix hasher"
//     stage to get a "prefix hash", meaning hash the first X bytes of the
//     local file and then store the hasher ready to accept more data. Or it
//     might pass things on to the downloader stage for a "suffix download",
//     meaning download everything after the first X bytes (in the degenerate
//     case, X could be zero, indicating a full download).
//   - The prefix hasher stage feeds into the downloader stage. If the sizes
//     are equal and the hashes are totally different, it'll truncate the file
//     and ask the downloader for a "suffix download" starting from zero. If
//     the local file is shorter than the file in Azure, it'll ask the
//     downloader for a "suffix download" starting from however big the local
//     file is. (Or, if the sizes are equal and the hashes are the same,
//     then yay, that's a terminal state!)
//   - The downloader stage can do suffix downloads, and if it determines
//     after it has done a suffix download that it also needs to do a prefix
//     download as well, then it can do that. If it does a prefix download
//     then it needs to pass it off to the "suffix hasher" stage. Or if the
//     hash is correct and it doesn't need to do a prefix download, then,
//     yay, that's terminal.
//   - The suffix hasher stage just makes sure that the complete file has
//     the hash we expect.

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

    let mut runner: wuffblob::runner::Runner<
        crate::state_machine::Downloader,
    > = wuffblob::runner::Runner::new(1000);

    Ok(())
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command =
        wuffblob::ctx::make_cmdline_parser("wuffblob-download")
            .arg(
                clap::Arg::new("paths")
                    .value_parser(clap::value_parser!(
                        wuffblob::path::WuffPath
                    ))
                    .action(clap::ArgAction::Append),
            )
            .arg(
                clap::Arg::new("download_as")
                    .long("download-as")
                    .value_parser(clap::value_parser!(std::path::PathBuf))
                    .action(clap::ArgAction::Set),
            );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let mut raw_to_download: Vec<wuffblob::path::WuffPath> = Vec::new();
    if let Some(raw_paths) =
        cmdline_matches.get_many::<wuffblob::path::WuffPath>("paths")
    {
        for raw_path in raw_paths {
            if let Some(path) =
                raw_path.clone().canonicalize_or_componentless()
            {
                raw_to_download.push(path);
            } else {
                return Err(
                    format!("{:?}: invalid remote path", raw_path).into()
                );
            }
        }
    };
    let mut to_download: Vec<(wuffblob::path::WuffPath, std::path::PathBuf)> =
        Vec::with_capacity(raw_to_download.len());
    if let Some(download_as) =
        cmdline_matches.get_one::<std::path::PathBuf>("download_as")
    {
        if raw_to_download.is_empty() {
            to_download
                .push((wuffblob::path::WuffPath::new(), download_as.clone()));
        } else if raw_to_download.len() == 1 {
            to_download
                .push((raw_to_download.pop().unwrap(), download_as.clone()));
        } else {
            return Err("If download-as is specified, there must be at most one remote path specified to download".into());
        }
    } else if raw_to_download.is_empty() {
        to_download.push((wuffblob::path::WuffPath::new(), ".".into()));
    } else {
        for path in raw_to_download {
            let download_as: std::path::PathBuf = (&path).into();
            to_download.push((path, download_as));
        }

        // Checking for overlap in the remote paths will catch some possible
        // cases of overlap, but not all of them. We will still have to check
        // the st_dev/st_ino in all the files we open, to make sure we don't
        // have the same file open twice under two different names.
        //
        // But it's easy to check for overlap in the remote paths, and it
        // might catch some of the simpler cases, like "derp, I told it to
        // download both foo/ and foo/bar/"
        let mut paths: Vec<&wuffblob::path::WuffPath> =
            Vec::with_capacity(to_download.len());
        for (path, _) in &to_download {
            paths.push(path);
        }
        wuffblob::path::WuffPath::check_for_overlap(paths)?;
    }

    let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::new(
        crate::ctx::Ctx::new(&cmdline_matches, to_download)?,
    );
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
