#[derive(Debug)]
pub struct Ctx<'a> {
    pub verbose: bool,
    pub dry_run: bool,
    pub async_spawner: &'a tokio::runtime::Handle,
    pub azure_client: crate::azure::AzureClient,
}

pub fn make_tokio_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio")
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

impl<'a> Ctx<'a> {
    pub fn new(tokio_runtime: &'a tokio::runtime::Runtime, matches: &clap::ArgMatches) -> Ctx<'a> {
        let storage_account: &String = matches.get_one::<String>("storage_account").unwrap();
        let container: &String = matches.get_one::<String>("container").unwrap();

        let mut access_key: String = String::new();
        if let Ok(s) = std::env::var("WUFFBLOB_ACCESS_KEY") {
            access_key = s;
        } else {
            print!("Enter access key for storage account {}: ", storage_account);
            std::io::Write::flush(&mut std::io::stdout()).expect("stdout");
            std::io::stdin().read_line(&mut access_key).expect("stdin");
        }

        access_key = String::from(access_key.trim());

        Ctx {
            verbose: *(matches.get_one::<bool>("verbose").unwrap()),
            dry_run: *(matches.get_one::<bool>("dry_run").unwrap()),
            async_spawner: tokio_runtime.handle(),
            azure_client: crate::azure::AzureClient::new(storage_account, &access_key, container),
        }
    }
}
