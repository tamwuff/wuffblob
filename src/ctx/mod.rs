#[derive(Debug)]
pub struct Ctx {
    pub verbose: bool,
    pub dry_run: bool,
    pub azure_client: crate::azure::AzureClient,
    tokio_runtime: tokio::runtime::Runtime,
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
        }
    }
    pub fn run_async_main<F>(&self, f: F) -> Result<(), crate::error::WuffBlobError>
    where
        F: std::future::Future<Output = Result<(), crate::error::WuffBlobError>>,
    {
        self.tokio_runtime.block_on(f)
    }

    pub fn get_async_spawner(&self) -> &tokio::runtime::Handle {
        self.tokio_runtime.handle()
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
