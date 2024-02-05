async fn async_main_foo(ctx: &wuffblob::ctx::Ctx<'_>) -> wuffblob::error::WuffBlobResult {
    let mut contents = ctx.container_client.list_blobs().into_stream();
    while let Some(elem) = futures::stream::StreamExt::next(&mut contents).await {
        match elem {
            Ok(list_blobs_response) => {
                println!("OK: {:#?}", &list_blobs_response);
            }
            Err(err) => {
                if ctx.verbose {
                    return Err(format!("{:?}", err).into());
                } else {
                    return Err(err.to_string().into());
                }
            }
        }
    }
    Ok(())
}

async fn async_main(ctx: &wuffblob::ctx::Ctx<'_>) -> wuffblob::error::WuffBlobResult {
    let mut contents = ctx.data_lake_client.list_paths().recursive(true).into_stream();
    while let Some(elem) = futures::stream::StreamExt::next(&mut contents).await {
        match elem {
            Ok(list_blobs_response) => {
                println!("OK: {:#?}", &list_blobs_response);
            }
            Err(err) => {
                if ctx.verbose {
                    return Err(format!("{:?}", err).into());
                } else {
                    return Err(err.to_string().into());
                }
            }
        }
    }
    Ok(())
}

fn main() -> wuffblob::error::WuffBlobResult {
    let tokio_runtime: tokio::runtime::Runtime = wuffblob::ctx::make_tokio_runtime();
    let cmdline_parser: clap::Command = wuffblob::ctx::make_cmdline_parser("wuffblob-fsck");
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let ctx: wuffblob::ctx::Ctx = wuffblob::ctx::Ctx::new(&tokio_runtime, &cmdline_matches);
    tokio_runtime.block_on(async_main(&ctx))
}
