#[derive(Debug)]
pub struct Ctx {
    pub verbose: bool,
    pub dry_run: bool,
    pub azure_client: crate::azure::AzureClient,
    tokio_runtime: tokio::runtime::Runtime,
    data_concurrency: u8,
    metadata_concurrency: u8,
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
    helper: std::sync::Arc<BoundedParallelismHelper<Result<T, crate::error::WuffBlobError>>>,
}

impl<T: std::marker::Send + 'static> BoundedParallelism<T> {
    pub fn new(parallelism: u8) -> Self {
        Self {
            parallelism: parallelism,
            helper: std::sync::Arc::<
                BoundedParallelismHelper<Result<T, crate::error::WuffBlobError>>,
            >::new(BoundedParallelismHelper::<
                Result<T, crate::error::WuffBlobError>,
            > {
                inside_mutex: std::sync::Mutex::<
                    BoundedParallelismHelperInsideMutex<Result<T, crate::error::WuffBlobError>>,
                >::new(BoundedParallelismHelperInsideMutex::<
                    Result<T, crate::error::WuffBlobError>,
                > {
                    currently_running: 0u8,
                    results: Vec::<Result<T, crate::error::WuffBlobError>>::new(),
                }),
                cv: tokio::sync::Notify::new(),
            }),
        }
    }

    pub async fn spawn<F>(
        &self,
        ctx: &std::sync::Arc<Ctx>,
        f: F,
    ) -> Vec<Result<T, crate::error::WuffBlobError>>
    where
        F: std::future::Future<Output = T> + std::marker::Send + 'static,
    {
        let mut results: Vec<Result<T, crate::error::WuffBlobError>> =
            Vec::<Result<T, crate::error::WuffBlobError>>::new();
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

    pub async fn drain(&self) -> Vec<Result<T, crate::error::WuffBlobError>> {
        let mut results: Vec<Result<T, crate::error::WuffBlobError>> =
            Vec::<Result<T, crate::error::WuffBlobError>>::new();
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
    pub fn new(cmdline_matches: &clap::ArgMatches) -> Ctx {
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

        Ctx {
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
        }
    }

    pub fn run_async_main<F>(&self, f: F) -> Result<(), crate::error::WuffBlobError>
    where
        F: std::future::Future<Output = Result<(), crate::error::WuffBlobError>>,
    {
        self.tokio_runtime.block_on(f)
    }

    pub fn install_siginfo_handler<F>(&self, cb: F) -> Result<(), crate::error::WuffBlobError>
    where
        F: Fn() + std::marker::Send + 'static,
    {
        let sig_num: tokio::signal::unix::SignalKind = tokio::signal::unix::SignalKind::info();
        let mut signal_waiter: tokio::signal::unix::Signal = tokio::signal::unix::signal(sig_num)?;
        let _ = self.get_async_spawner().spawn(async move {
            loop {
                signal_waiter.recv().await;
                cb();
            }
        });
        Ok(())
    }

    pub fn get_async_spawner(&self) -> &tokio::runtime::Handle {
        self.tokio_runtime.handle()
    }

    pub fn data_concurrency_mgr<T: std::marker::Send + 'static>(&self) -> BoundedParallelism<T> {
        return BoundedParallelism::<T>::new(self.data_concurrency);
    }

    pub fn metadata_concurrency_mgr<T: std::marker::Send + 'static>(
        &self,
    ) -> BoundedParallelism<T> {
        return BoundedParallelism::<T>::new(self.metadata_concurrency);
    }

    pub fn get_desired_mime_type(&self, path: &crate::wuffpath::WuffPath) -> &'static str {
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
