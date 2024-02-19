pub struct Ctx {
    pub verbose: bool,
    pub dry_run: bool,
    pub azure_client: crate::azure::AzureClient,
    tokio_runtime: tokio::runtime::Runtime,
    data_concurrency: u8,
    metadata_concurrency: u8,
    mime_type_guessers: Vec<Box<dyn crate::mimetypes::MimeTypes + Sync + Send>>,
}

struct BoundedParallelismHelperInsideMutex<T> {
    currently_running: u8,
    results: Vec<T>,
}

struct BoundedParallelismHelper<T> {
    inside_mutex: std::sync::Mutex<BoundedParallelismHelperInsideMutex<T>>,

    // tokio doesn't have real condition variables. this is as close as it gets
    cv: tokio::sync::Notify,
}

pub struct BoundedParallelism<T> {
    parallelism: u8,
    helper: std::sync::Arc<BoundedParallelismHelper<Result<T, crate::error::WuffError>>>,
}

impl<T: Send + 'static> BoundedParallelism<T> {
    pub fn new(parallelism: u8) -> Self {
        Self {
            parallelism: parallelism,
            helper: std::sync::Arc::<
                BoundedParallelismHelper<Result<T, crate::error::WuffError>>,
            >::new(BoundedParallelismHelper::<
                Result<T, crate::error::WuffError>,
            > {
                inside_mutex: std::sync::Mutex::<
                    BoundedParallelismHelperInsideMutex<Result<T, crate::error::WuffError>>,
                >::new(BoundedParallelismHelperInsideMutex::<
                    Result<T, crate::error::WuffError>,
                > {
                    currently_running: 0u8,
                    results: Vec::<Result<T, crate::error::WuffError>>::new(),
                }),
                cv: tokio::sync::Notify::new(),
            }),
        }
    }

    pub async fn spawn<F>(
        &self,
        ctx: &std::sync::Arc<Ctx>,
        f: F,
    ) -> Vec<Result<T, crate::error::WuffError>>
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let mut results: Vec<Result<T, crate::error::WuffError>> =
            Vec::<Result<T, crate::error::WuffError>>::new();
        loop {
            {
                let mut inside_mutex = self.helper.inside_mutex.lock().expect("BoundedParallelism");
                if inside_mutex.currently_running < self.parallelism {
                    inside_mutex.currently_running += 1;

                    let task: tokio::task::JoinHandle<T> = ctx.get_async_spawner().spawn(f);

                    let helper_for_watcher = std::sync::Arc::clone(&self.helper);
                    let fut = async move {
                        let result = task.await;
                        let mut inside_for_watcher = helper_for_watcher
                            .inside_mutex
                            .lock()
                            .expect("BoundedParallelism");
                        inside_for_watcher.currently_running -= 1;
                        inside_for_watcher.results.push(match result {
                            Ok(x) => Ok(x),
                            Err(e) => Err(e.into()),
                        });
                        helper_for_watcher.cv.notify_one();
                    };
                    let _ = ctx.get_async_spawner().spawn(fut);

                    std::mem::swap(&mut results, &mut inside_mutex.results);
                    break;
                }
            }
            self.helper.cv.notified().await
        }
        results
    }

    pub async fn drain(&self) -> Vec<Result<T, crate::error::WuffError>> {
        let mut results: Vec<Result<T, crate::error::WuffError>> =
            Vec::<Result<T, crate::error::WuffError>>::new();
        loop {
            {
                let mut inside_mutex = self.helper.inside_mutex.lock().expect("BoundedParallelism");
                if inside_mutex.currently_running == 0u8 {
                    std::mem::swap(&mut results, &mut inside_mutex.results);
                    break;
                }
            }
            self.helper.cv.notified().await
        }
        results
    }
}

// I looked on https://docs.rs/platforms/latest/platforms/target/enum.OS.html
// and tried to identify any BSD-like platforms, but I may have missed some.
#[cfg(any(
    target_os = "dragonfly",
    target_os = "freebsd",
    target_os = "netbsd",
    target_os = "openbsd",
    target_os = "macos"
))]
fn what_is_siginfo_on_this_platform() -> tokio::signal::unix::SignalKind {
    tokio::signal::unix::SignalKind::info()
}
#[cfg(all(
    unix,
    not(any(
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "netbsd",
        target_os = "openbsd",
        target_os = "macos"
    ))
))]
fn what_is_siginfo_on_this_platform() -> tokio::signal::unix::SignalKind {
    tokio::signal::unix::SignalKind::user_defined1()
}

pub fn make_cmdline_parser(argv0: &'static str) -> clap::Command {
    clap::Command::new(argv0)
        .color(clap::ColorChoice::Never)
        .arg(
            clap::Arg::new("verbose")
                .long("verbose")
                .short('v')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            clap::Arg::new("dry_run")
                .long("dry-run")
                .short('n')
                .action(clap::ArgAction::SetTrue),
        )
        .arg(
            clap::Arg::new("storage_account")
                .long("storage-account")
                .env("WUFFBLOB_STORAGE_ACCOUNT")
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            clap::Arg::new("container")
                .long("container")
                .env("WUFFBLOB_CONTAINER")
                .required(true)
                .action(clap::ArgAction::Set),
        )
        .arg(
            clap::Arg::new("data_concurrency")
                .long("data-concurrency")
                .value_parser(clap::value_parser!(u8).range(1..=10))
                .default_value("2")
                .action(clap::ArgAction::Set),
        )
        .arg(
            clap::Arg::new("metadata_concurrency")
                .long("metadata-concurrency")
                .value_parser(clap::value_parser!(u8).range(1..=100))
                .default_value("20")
                .action(clap::ArgAction::Set),
        )
        .arg(
            clap::Arg::new("mime_types")
                .long("mime-types")
                .value_parser(clap::value_parser!(std::path::PathBuf))
                .action(clap::ArgAction::Set),
        )
        .arg(
            clap::Arg::new("mime_types_regex")
                .long("mime-types-regex")
                .value_parser(clap::value_parser!(std::path::PathBuf))
                .action(clap::ArgAction::Set),
        )
}

static HEX_DIGITS: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];
pub fn hex_encode(buf: &[u8]) -> String {
    let mut s: String = String::with_capacity(buf.len() * 2);
    for i in buf {
        s.push(HEX_DIGITS[(i >> 4) as usize]);
        s.push(HEX_DIGITS[(i & 0xfu8) as usize]);
    }
    s
}

impl Ctx {
    pub fn new(cmdline_matches: &clap::ArgMatches) -> Result<Ctx, crate::error::WuffError> {
        let storage_account: &String = cmdline_matches
            .get_one::<String>("storage_account")
            .unwrap();
        let container: &String = cmdline_matches.get_one::<String>("container").unwrap();

        let mut access_key: String = String::new();
        if let Ok(s) = std::env::var("WUFFBLOB_ACCESS_KEY") {
            access_key = s;
        } else {
            print!("Enter access key for storage account {}: ", storage_account);
            std::io::Write::flush(&mut std::io::stdout().lock()).expect("stdout");
            std::io::BufRead::read_line(&mut std::io::stdin().lock(), &mut access_key)
                .expect("stdin");
        }

        access_key = String::from(access_key.trim());

        let mut mime_type_guessers: Vec<Box<dyn crate::mimetypes::MimeTypes + Sync + Send>> =
            Vec::<Box<dyn crate::mimetypes::MimeTypes + Sync + Send>>::new();
        // If the user has passed in their own, add them first so they will be
        // checked first
        if let Some(mime_types_filename) =
            cmdline_matches.get_one::<std::path::PathBuf>("mime_types_regex")
        {
            let contents: &'static str =
                Box::leak(std::fs::read_to_string(mime_types_filename)?.into_boxed_str());
            mime_type_guessers.push(crate::mimetypes::new(contents, true)?);
        }
        if let Some(mime_types_filename) =
            cmdline_matches.get_one::<std::path::PathBuf>("mime_types")
        {
            let contents: &'static str =
                Box::leak(std::fs::read_to_string(mime_types_filename)?.into_boxed_str());
            mime_type_guessers.push(crate::mimetypes::new(contents, false)?);
        }
        // OK, now put in the standard ones.
        mime_type_guessers.push(crate::mimetypes::new(
            crate::mimetypes::DEFAULT_REGEX,
            true,
        )?);
        mime_type_guessers.push(crate::mimetypes::new(
            crate::mimetypes::DEFAULT_FIXED,
            false,
        )?);

        Ok(Ctx {
            verbose: *(cmdline_matches.get_one::<bool>("verbose").unwrap()),
            dry_run: *(cmdline_matches.get_one::<bool>("dry_run").unwrap()),
            azure_client: crate::azure::AzureClient::new(storage_account, &access_key, container),
            tokio_runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("tokio"),
            data_concurrency: *(cmdline_matches.get_one::<u8>("data_concurrency").unwrap()),
            metadata_concurrency: *(cmdline_matches
                .get_one::<u8>("metadata_concurrency")
                .unwrap()),
            mime_type_guessers: mime_type_guessers,
        })
    }

    pub fn run_async_main<F>(&self, f: F) -> Result<(), crate::error::WuffError>
    where
        F: std::future::Future<Output = Result<(), crate::error::WuffError>>,
    {
        self.tokio_runtime.block_on(f)
    }

    #[cfg(unix)]
    pub fn install_siginfo_handler<F>(&self, cb: F) -> Result<(), crate::error::WuffError>
    where
        F: Fn() + Send + 'static,
    {
        let sig_num: tokio::signal::unix::SignalKind = what_is_siginfo_on_this_platform();
        let mut signal_waiter: tokio::signal::unix::Signal = tokio::signal::unix::signal(sig_num)?;
        let _ = self.get_async_spawner().spawn(async move {
            loop {
                signal_waiter.recv().await;
                cb();
            }
        });
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn install_siginfo_handler<F>(&self, cb: F) -> Result<(), crate::error::WuffError>
    where
        F: Fn() + Send + 'static,
    {
    }

    pub fn get_async_spawner(&self) -> &tokio::runtime::Handle {
        self.tokio_runtime.handle()
    }

    pub fn data_concurrency_mgr<T: Send + 'static>(&self) -> BoundedParallelism<T> {
        return BoundedParallelism::<T>::new(self.data_concurrency);
    }

    pub fn metadata_concurrency_mgr<T: Send + 'static>(&self) -> BoundedParallelism<T> {
        return BoundedParallelism::<T>::new(self.metadata_concurrency);
    }

    pub fn get_desired_mime_type(&self, path: &crate::path::WuffPath) -> &'static str {
        if let Some(basename) = path.components.last() {
            for mime_type_guesser in &self.mime_type_guessers {
                if let Some(mime_type) = mime_type_guesser.get_desired_mime_type(basename) {
                    return mime_type;
                }
            }
        }
        "application/octet-stream"
    }
}

#[test]
fn hex_empty() {
    let v = Vec::<u8>::new();
    let s = hex_encode(&v);
    assert_eq!(s, "");
}

#[test]
fn hex_nonempty() {
    let v = vec![185u8, 74u8, 155u8, 38u8, 162u8];
    let s = hex_encode(&v);
    assert_eq!(s, "b94a9b26a2");
}
