#[derive(Debug, Clone)]
pub struct Stats {
    pub files_found: u64,
    pub bytes_found: u64,
    pub files_queried_in_cloud: u64,
    pub done_listing: bool,
    pub files_need_hashing_locally: u64,
    pub bytes_need_hashing_locally: u64,
    pub files_completed_hashing_locally: u64,
    pub bytes_completed_hashing_locally: u64,
    pub files_can_reuse: u64,
    pub bytes_can_reuse: u64,
    pub files_need_download: u64,
    pub bytes_need_download: u64,
    pub files_downloaded: u64,
    pub bytes_downloaded: u64,
    pub files_verified: u64,
    pub bytes_verified: u64,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            files_found: 0u64,
            bytes_found: 0u64,
            files_queried_in_cloud: 0u64,
            done_listing: false,
            files_need_hashing_locally: 0u64,
            bytes_need_hashing_locally: 0u64,
            files_completed_hashing_locally: 0u64,
            bytes_completed_hashing_locally: 0u64,
            files_can_reuse: 0u64,
            bytes_can_reuse: 0u64,
            files_need_download: 0u64,
            bytes_need_download: 0u64,
            files_downloaded: 0u64,
            bytes_downloaded: 0u64,
            files_verified: 0u64,
            bytes_verified: 0u64,
        }
    }
}

// It would be easy for someone to use clever symlinks to trick us into
// downloading X into Y, and also downloading Z into Y. Unfortunately we
// can't reliably detect that without using unbounded memory. What we can
// do in constant memory is to enforce a weaker guarantee, which is that
// all of the local files *currently in flight, right now* will be unique.
//
// Enforcing that weaker constraint doesn't really help the user. The user
// will still be able to shoot themselves in the foot. But enforcing the
// weaker constraint does help us, in the sense that it allows us to make
// assumptions about the files not randomly growing or shrinking while we're
// trying to do the delicate work of, say, resuming a previously failed
// download.
//
// On unix, we use a tuple of (st_dev, st_ino). On non-unix, we use PathBuf
// and take advantage of std::fs::canonicalize().
#[cfg(unix)]
pub type DevIno = (u64, u64);
#[cfg(not(unix))]
pub type DevIno = std::path::PathBuf;

pub struct Ctx {
    pub base_ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    pub to_download: Vec<(wuffblob::path::WuffPath, std::path::PathBuf)>,
    pub stats: std::sync::Mutex<Stats>,
    pub in_flight: std::sync::Mutex<
        std::collections::BTreeMap<
            DevIno,
            (wuffblob::path::WuffPath, std::path::PathBuf),
        >,
    >,
    pub local_io_bound_operations: std::sync::Mutex<()>,
}

impl Ctx {
    pub fn new(
        cmdline_matches: &clap::ArgMatches,
        to_download: Vec<(wuffblob::path::WuffPath, std::path::PathBuf)>,
    ) -> Result<Ctx, wuffblob::error::WuffError> {
        Ok(Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new(
                &cmdline_matches,
            )?),
            to_download: to_download,
            stats: std::sync::Mutex::new(crate::ctx::Stats::new()),
            in_flight: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            local_io_bound_operations: std::sync::Mutex::new(()),
        })
    }

    // for unit tests
    #[cfg(test)]
    pub fn new_minimal(from_path: &str, to_path: &str) -> Ctx {
        Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new_minimal()),
            to_download: vec![(to_path.into(), from_path.into())],
            stats: std::sync::Mutex::new(crate::ctx::Stats::new()),
            in_flight: std::sync::Mutex::new(std::collections::BTreeMap::new()),
            local_io_bound_operations: std::sync::Mutex::new(()),
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

    print!("{}", s);
}
