#[derive(Debug, Clone)]
pub struct Stats {
    pub files_found: u64,
    pub bytes_found: u64,
    pub dirs_found: u64,
    pub done_listing: bool,
    pub any_propupd_attempted: bool,
    pub any_not_repaired: bool,
    pub user_input_required: u64,
    pub user_input_complete: u64,
    pub hash_required: u64,
    pub hash_complete: u64,
    pub hash_bytes_required: u64,
    pub hash_bytes_complete: u64,
    pub propupd_required: u64,
    pub propupd_complete: u64,
}

impl Stats {
    pub fn new() -> Stats {
        Stats {
            files_found: 0u64,
            bytes_found: 0u64,
            dirs_found: 0u64,
            done_listing: false,
            any_propupd_attempted: false,
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

pub struct Ctx {
    pub base_ctx: std::sync::Arc<wuffblob::ctx::Ctx>,
    pub preen: bool,
    pub yes: bool,
    pub stats: std::sync::Mutex<Stats>,
}

impl Ctx {
    pub fn new(
        cmdline_matches: &clap::ArgMatches,
    ) -> Result<Ctx, wuffblob::error::WuffError> {
        Ok(Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new(
                &cmdline_matches,
            )?),
            preen: *(cmdline_matches.get_one::<bool>("preen").unwrap()),
            yes: *(cmdline_matches.get_one::<bool>("yes").unwrap()),
            stats: std::sync::Mutex::new(crate::ctx::Stats::new()),
        })
    }

    // for unit tests
    #[cfg(test)]
    pub fn new_minimal() -> Ctx {
        Ctx {
            base_ctx: std::sync::Arc::new(wuffblob::ctx::Ctx::new_minimal()),
            preen: false,
            yes: false,
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
}
