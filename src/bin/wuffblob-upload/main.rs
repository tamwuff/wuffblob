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
//     in the graph and it's just easier to keep things linear.
//
// The error queue should only be used for errors that relate directly to
// files on the local filesystem or in the cloud. It shouldn't be used for
// "failed to write to queue" types of things. Just panic in that case.
// The main thread will collect everyone's panics and all the errors, and
// it will decide how to present all of it to the user.

#[derive(Debug, Clone)]
struct Stats {
    files_found: u64,
    bytes_found: u64,
    files_queried_in_cloud: u64,
    done_listing: bool,
    done_querying: bool,
    files_need_hashing_locally: u64,
    bytes_need_hashing_locally: u64,
    files_completed_hashing_locally: u64,
    bytes_completed_hashing_locally: u64,
    files_can_reuse: u64,
    bytes_can_reuse: u64,
    files_need_upload: u64,
    bytes_need_upload: u64,
    files_uploaded: u64,
    bytes_uploaded: u64,
}

impl Stats {
    fn new() -> Stats {
        Stats {
            files_found: 0u64,
            bytes_found: 0u64,
            files_queried_in_cloud: 0u64,
            done_listing: false,
            done_querying: false,
            files_need_hashing_locally: 0u64,
            bytes_need_hashing_locally: 0u64,
            files_completed_hashing_locally: 0u64,
            bytes_completed_hashing_locally: 0u64,
            files_can_reuse: 0u64,
            bytes_can_reuse: 0u64,
            files_need_upload: 0u64,
            bytes_need_upload: 0u64,
            files_uploaded: 0u64,
            bytes_uploaded: 0u64,
        }
    }
}

struct Ctx {
    base_ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)>,
    force: bool,
    stats: std::sync::Mutex<Stats>,
}

impl Ctx {
    fn get_stats(&self) -> Stats {
        self.stats.lock().expect("stats").clone()
    }

    fn mutate_stats<F>(&self, cb: F)
    where
        F: FnOnce(&mut Stats),
    {
        cb(&mut self.stats.lock().expect("stats"));
    }
}

#[derive(Debug, Clone)]
enum UploaderState {
    // Success! I don't need to be put into any queue.
    Ok,

    // Please put me into the error queue.
    Err(String),

    // Please put me into the metadata queue.
    NeedGet,

    // Please put me into the metadata queue.
    NeedMkdir,

    // Please put me into the queue to be hashed.
    Hash,

    // Please put me into the queue to be uploaded.
    Upload,
}

#[derive(Debug)]
struct Uploader {
    local_path: std::path::PathBuf,
    remote_path: wuffblob::path::WuffPath,
    state: UploaderState,
    local_metadata: std::fs::Metadata,
    remote_metadata: Option<azure_storage_blobs::blob::BlobProperties>,
    desired_content_type: &'static str,
    local_md5: Option<[u8; 16]>,
}

impl Uploader {
    fn new(
        ctx: &std::sync::Arc<Ctx>,
        local_path: std::path::PathBuf,
        remote_path: wuffblob::path::WuffPath,
        local_metadata: std::fs::Metadata,
    ) -> Uploader {
        // They were only supposed to give us canonical paths that were
        // either files or directories. Just double check...
        assert!(remote_path.is_canonical());
        let file_type: std::fs::FileType = local_metadata.file_type();
        assert!(file_type.is_file() || file_type.is_dir());

        Uploader {
            local_path: local_path,
            remote_path: remote_path,
            state: UploaderState::NeedGet,
            local_metadata: local_metadata,
            remote_metadata: None,
            desired_content_type: "",
            local_md5: None,
        }
    }
}

fn siginfo_handler(ctx: &std::sync::Arc<Ctx>) {
    let stats: Stats = ctx.get_stats();

    let mut s: String = String::new();
    s.push_str("\n\n");
    if stats.done_querying {
        assert!(stats.done_listing);
        assert_eq!(stats.files_found, stats.files_queried_in_cloud);
        s.push_str(&format!(
            "{} files and directories (final count)\n",
            stats.files_found
        ));
    } else if stats.done_listing {
        s.push_str(&format!(
            "Queried {} out of {} files and directories\n",
            stats.files_queried_in_cloud, stats.files_found
        ));
    } else {
        s.push_str(&format!(
            "Queried {} out of {} found so far (list not yet complete)\n",
            stats.files_queried_in_cloud, stats.files_found
        ));
    }
    if stats.files_need_hashing_locally > 0u64 {
        s.push_str(&format!(
            "Hashing: {} of {} ({} of {} bytes)\n",
            stats.files_completed_hashing_locally,
            stats.files_need_hashing_locally,
            stats.bytes_completed_hashing_locally,
            stats.bytes_need_hashing_locally
        ));
    }
    s.push_str("\n");

    if stats.files_found > 0u64 {
        s.push_str(&format!(
            "Can reuse: {} of {} ({} of {} bytes)\n",
            stats.files_can_reuse, stats.files_found, stats.bytes_can_reuse, stats.bytes_found
        ));
        s.push_str(&format!(
            "Need to upload: {} of {} ({} of {} bytes)\n",
            stats.files_need_upload, stats.files_found, stats.bytes_need_upload, stats.bytes_found
        ));
        s.push_str("\n");
    }

    if stats.files_need_upload > 0u64 {
        s.push_str(&format!(
            "Uploaded: {} of {} ({} of {} bytes)\n",
            stats.files_uploaded,
            stats.files_need_upload,
            stats.bytes_uploaded,
            stats.bytes_need_upload,
        ));
        s.push_str("\n");
    }

    print!("{}", s);
}

async fn async_main(ctx: std::sync::Arc<Ctx>) -> Result<(), wuffblob::error::WuffError> {
    ctx.base_ctx.install_siginfo_handler({
        let ctx: std::sync::Arc<Ctx> = std::sync::Arc::clone(&ctx);
        move || {
            siginfo_handler(&ctx);
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
                .required(true)
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
        );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let raw_to_upload: Vec<&std::path::PathBuf> = Vec::from_iter(
        cmdline_matches
            .get_many::<std::path::PathBuf>("paths")
            .unwrap(),
    );
    let mut to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(raw_to_upload.len());
    if let Some(raw_upload_as) = cmdline_matches.get_one::<wuffblob::path::WuffPath>("upload_as") {
        if raw_to_upload.len() != 1 {
            return Err("If upload-as is specified, there must be exactly one local path specified to upload".into());
        }
        if let Some(upload_as) = raw_upload_as.canonicalize() {
            to_upload.push((raw_to_upload[0].clone(), upload_as));
        } else {
            return Err(format!(
                "Upload-as path given as {} which is not canonicalizable",
                raw_upload_as
            )
            .into());
        }
    } else {
        for path in raw_to_upload {
            if let Some(upload_as) =
                TryInto::<wuffblob::path::WuffPath>::try_into(path.as_path())?.canonicalize()
            {
                to_upload.push((path.clone(), upload_as));
            } else {
                return Err(format!("{:?} is not canonicalizable (try --upload-as)", path).into());
            }
        }
    }

    let ctx: std::sync::Arc<Ctx> = std::sync::Arc::new(Ctx {
        base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new(&cmdline_matches)?),
        to_upload: to_upload,
        force: *(cmdline_matches.get_one::<bool>("force").unwrap()),
        stats: std::sync::Mutex::new(Stats::new()),
    });
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
