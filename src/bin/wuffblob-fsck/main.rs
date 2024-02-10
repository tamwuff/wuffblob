async fn async_main(ctx: &wuffblob::ctx::Ctx<'_>) -> Result<(), wuffblob::error::WuffBlobError> {
    let inv = ctx.azure_client.inventory(ctx).await?;
    Ok(())
}

fn main() -> Result<(), wuffblob::error::WuffBlobError> {
    let tokio_runtime: tokio::runtime::Runtime = wuffblob::ctx::make_tokio_runtime();
    let cmdline_parser: clap::Command = wuffblob::ctx::make_cmdline_parser("wuffblob-fsck");
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let ctx: wuffblob::ctx::Ctx = wuffblob::ctx::Ctx::new(&tokio_runtime, &cmdline_matches);
    tokio_runtime.block_on(async_main(&ctx))
}
