#[derive(Debug, Clone)]
pub struct Stats {
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
    pub fn new() -> Stats {
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

pub struct Ctx {
    pub base_ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    pub to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)>,
    pub force: bool,
    pub stats: std::sync::Mutex<Stats>,
}

impl Ctx {
    pub fn new(
        cmdline_matches: &clap::ArgMatches,
        to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)>,
    ) -> Result<Ctx, wuffblob::error::WuffError> {
        Ok(Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new(&cmdline_matches)?),
            to_upload: to_upload,
            force: *(cmdline_matches.get_one::<bool>("force").unwrap()),
            stats: std::sync::Mutex::new(crate::ctx::Stats::new()),
        })
    }

    // for unit tests
    #[cfg(test)]
    pub fn new_minimal(from_path: &str, to_path: &str) -> Ctx {
        Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new_minimal()),
            to_upload: vec![(from_path.into(), to_path.into())],
            force: false,
            stats: std::sync::Mutex::new(crate::ctx::Stats::new()),
        }
    }

    pub fn get_stats(&self) -> Stats {
        self.stats.lock().expect("stats").clone()
    }

    pub fn mutate_stats<F>(&self, cb: F)
    where
        F: FnOnce(&mut Stats),
    {
        cb(&mut self.stats.lock().expect("stats"));
    }
}

pub fn siginfo_handler(ctx: &std::sync::Arc<Ctx>) {
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
