// Useful reference for all the ergonomics of how an fsck-like thing ought
// to behave:
// https://cgit.freebsd.org/src/tree/sbin/fsck_ffs?id=771a16a90f448bb28f827ec47322471859cfb7aa (as of 1995)
// https://cgit.freebsd.org/src/diff/?id=6db798cae4e1313dd0ef9b2bcaceca5ee73c75fd (SIGINFO support)
//
// In our case, Azure seems to follow the same (or stricter) path
// canonicalization rules as we do, so we don't have to write code to
// *fix* non-canonical paths. We should check for them, but it's ok to
// just error out if we find any, we don't have to try to repair them.
//
// That also means we don't have to pay much attention to directories. The
// only thing we can really do with a directory is make sure its path is
// canonical...
//
// In our case, preen mode will mean to do as much as we can without user
// input and without taking *too* horribly long. Non-preen mode will download
// everything and check the MD5s, so it will take longer, but it will give
// extra confidence that the data is ok. In non-preen mode we will also ask
// permission before we do anything.
//
// In all modes, we will iterate through all blobs and check:
//   - content type (it should match what we think it should be)
//   - MD5 (it should have one; if it doesn't, start hashing in the background)
// Additionally, in non-preen mode, we will:
//   - check the hash even if it had an MD5 already
//
// Azure does not let us modify:
//   - etag
//
// We don't consider it our purview to examine or modify:
//   - content disposition
//   - cache control
//   - content language
//   - access tier
//
// The way the stages work:
//   - The main thread feeds things into either the UI stage or the hasher
//     stage
//   - The hasher stage feeds things into the UI stage
//   - That means everything is now going into a single stream into the UI
//     stage
//   - The UI stage feeds things into the properties updater stage

mod ctx;
mod state_machine;

async fn do_hash(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut checker: Box<crate::state_machine::Checker>,
) -> Box<crate::state_machine::Checker> {
    let filename_as_string: String = match checker
        .path
        .to_osstring()
        .into_string()
    {
        Ok(s) => s,
        Err(_) => {
            checker.hash_failed(
                &ctx,
                wuffblob::error::WuffError::from("path is not valid unicode"),
            );
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.hash_required -= 1u64;
                stats.hash_bytes_required -= checker.properties.content_length;
            });
            return checker;
        }
    };
    let blob_client = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(filename_as_string);
    let mut chunks_stream = blob_client.get().into_stream();
    let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
    let expected_len: u64 = checker.properties.content_length;
    let mut num_bytes_hashed: u64 = 0u64;
    // We desperately do not want to move anything here, because moves
    // mean copying. We borrow even when it would seem to be in our best
    // interest to consume...!
    while let Some(ref mut maybe_chunk) =
        futures::stream::StreamExt::next(&mut chunks_stream).await
    {
        match maybe_chunk {
            Ok(ref mut chunk) => {
                let chunk_stream = &mut chunk.data;
                while let Some(ref maybe_buf) =
                    futures::stream::StreamExt::next(chunk_stream).await
                {
                    match maybe_buf {
                        Ok(ref buf) => {
                            let buf_len: usize = buf.len();
                            <md5::Md5 as md5::Digest>::update(
                                &mut hasher,
                                buf,
                            );
                            num_bytes_hashed += buf_len as u64;
                            ctx.mutate_stats(
                                |stats: &mut crate::ctx::Stats| {
                                    stats.hash_bytes_complete +=
                                        buf_len as u64;
                                },
                            );
                        }
                        Err(err) => {
                            checker.hash_failed(
                                &ctx,
                                wuffblob::error::WuffError::from(err),
                            );
                            ctx.mutate_stats(
                                |stats: &mut crate::ctx::Stats| {
                                    stats.hash_bytes_complete -=
                                        num_bytes_hashed;
                                    stats.hash_required -= 1u64;
                                    stats.hash_bytes_required -= expected_len;
                                },
                            );
                            return checker;
                        }
                    }
                }
            }
            Err(ref err) => {
                checker
                    .hash_failed(&ctx, wuffblob::error::WuffError::from(err));
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.hash_bytes_complete -= num_bytes_hashed;
                    stats.hash_required -= 1u64;
                    stats.hash_bytes_required -= expected_len;
                });
                return checker;
            }
        }
    }
    if num_bytes_hashed != expected_len {
        checker.hash_failed(
            &ctx,
            wuffblob::error::WuffError::from(format!(
                "Expected {} bytes, got {} instead",
                expected_len, num_bytes_hashed
            )),
        );
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.hash_bytes_complete -= num_bytes_hashed;
            stats.hash_required -= 1u64;
            stats.hash_bytes_required -= expected_len;
        });
        return checker;
    }
    checker.provide_hash(
        &ctx,
        <md5::Md5 as md5::Digest>::finalize(hasher)
            .as_slice()
            .try_into()
            .unwrap(),
    );
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.hash_complete += 1;
    });
    if let crate::state_machine::CheckerState::UserInteraction(_) =
        checker.state
    {
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.user_input_required += 1;
        });
    }
    checker
}

fn do_ui(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    checker: &mut crate::state_machine::Checker,
) {
    let crate::state_machine::CheckerState::UserInteraction(
        ref proposed_repair,
    ) = checker.state
    else {
        panic!(
            "unexpected file checker state {:?} in UI thread",
            &checker.state
        );
    };
    let name: std::ffi::OsString = checker.path.to_osstring();
    let _ui_lock = ctx.user_interaction.lock();
    print!("{:?}: {}", name, proposed_repair.problem_statement);
    if ctx.base_ctx.dry_run {
        print!("\n{}? no\n\n", proposed_repair.question);
        checker.provide_user_input(&ctx, false);
    } else if ctx.preen {
        println!(" ({})", proposed_repair.action);
        checker.provide_user_input(&ctx, true);
    } else if ctx.yes {
        print!("\n{}? yes\n\n", proposed_repair.question);
        checker.provide_user_input(&ctx, true);
    } else {
        print!("\n{}? [yn] ", proposed_repair.question);
        std::io::Write::flush(&mut std::io::stdout().lock()).expect("stdout");
        let answer: bool = loop {
            let mut s = String::new();
            if let Ok(_) = std::io::BufRead::read_line(
                &mut std::io::stdin().lock(),
                &mut s,
            ) {
                s = String::from(s.trim());
                if s.eq_ignore_ascii_case("y") {
                    break true;
                } else if s.eq_ignore_ascii_case("n") {
                    break false;
                }
            } else {
                return;
            }
        };
        checker.provide_user_input(&ctx, answer);
    }
    if !matches!(
        checker.state,
        crate::state_machine::CheckerState::UserInteraction(_)
    ) {
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.user_input_complete += 1u64;
        });
    }
    if let crate::state_machine::CheckerState::UpdateProperties(_) =
        checker.state
    {
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.propupd_required += 1u64;
        });
    }
}

async fn do_update_properties(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut checker: Box<crate::state_machine::Checker>,
) -> Box<crate::state_machine::Checker> {
    let crate::state_machine::CheckerState::UpdateProperties(
        ref desired_props,
    ) = checker.state
    else {
        panic!(
            "State is {:?}, expected CheckerState::UpdateProperties",
            &checker.state
        );
    };
    let filename_as_string: String =
        match checker.path.to_osstring().into_string() {
            Ok(s) => s,
            Err(_) => {
                checker.update_properties_failed(
                    &ctx,
                    wuffblob::error::WuffError::from(
                        "path is not valid unicode",
                    ),
                );
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.propupd_required -= 1;
                });
                return checker;
            }
        };
    let blob_client = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(filename_as_string);
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.any_propupd_attempted = true;
    });
    match blob_client
        .set_properties()
        .set_from_blob_properties(desired_props.clone())
        .into_future()
        .await
    {
        Ok(_) => {
            checker.properties_updated();
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.propupd_complete += 1;
            });
        }
        Err(err) => {
            checker.update_properties_failed(
                &ctx,
                wuffblob::error::WuffError::from(err),
            );
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.propupd_required -= 1;
            });
        }
    }
    checker
}

async fn feed(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    writer: tokio::sync::mpsc::Sender<Box<crate::state_machine::Checker>>,
) -> Result<(), wuffblob::error::WuffError> {
    let mut contents = ctx
        .base_ctx
        .azure_client
        .container_client
        .list_blobs()
        .into_stream();
    while let Some(possible_chunk) =
        futures::stream::StreamExt::next(&mut contents).await
    {
        // We want to consume 'possible_chunk'. Normally we would be very
        // careful when iterating, *not* to consume what we are iterating
        // over. Here we are equally careful *to* consume it.
        let chunk: azure_storage_blobs::container::operations::list_blobs::ListBlobsResponse =
            possible_chunk?;

        // chunk.blobs.blobs() is a nice convenience function but it only
        // returns refs... we want to consume...
        for blob_item in chunk.blobs.items {
            if let azure_storage_blobs::container::operations::BlobItem::Blob(
                blob,
            ) = blob_item
            {
                let is_dir: bool = wuffblob::util::blob_is_dir(&blob);
                if is_dir {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.dirs_found += 1u64;
                    });
                } else {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.files_found += 1u64;
                        stats.bytes_found += blob.properties.content_length;
                    });
                }

                // Here's the big payoff for us having consumed everything:
                // we get to move blob.properties. Yay!
                let checker: Box<crate::state_machine::Checker> =
                    Box::new(crate::state_machine::Checker::new(
                        &ctx,
                        wuffblob::path::WuffPath::from_osstr(
                            std::ffi::OsStr::new(&blob.name),
                        ),
                        is_dir,
                        blob.properties,
                    ));
                match checker.state {
                    crate::state_machine::CheckerState::Hash => {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.hash_required += 1u64;
                            stats.hash_bytes_required +=
                                checker.properties.content_length;
                        });
                    }
                    crate::state_machine::CheckerState::UserInteraction(_) => {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.user_input_required += 1u64;
                        });
                    }
                    _ => {}
                }
                writer.send(checker).await.expect("reader died");
            }
        }
    }
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.done_listing = true;
    });
    Ok(())
}

async fn async_main(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
) -> Result<(), wuffblob::error::WuffError> {
    ctx.base_ctx.install_siginfo_handler({
        let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
        move || {
            crate::ctx::siginfo_handler(&ctx);
        }
    })?;

    let mut runner: wuffblob::runner::Runner<crate::state_machine::Checker> =
        wuffblob::runner::Runner::new();

    runner.handle_terminal(
        &ctx.base_ctx,
        |checker: &crate::state_machine::Checker| matches!(checker.state, crate::state_machine::CheckerState::Ok | crate::state_machine::CheckerState::Error(None)),
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
            move |checker: Box<crate::state_machine::Checker>| -> Result<(), wuffblob::error::WuffError> {
                if matches!(checker.state, crate::state_machine::CheckerState::Error(_)) {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.any_not_repaired = true;
                    });
                }
                Ok(())
            }
        },
        1000,
    );

    runner.handle_terminal(
        &ctx.base_ctx,
        |checker: &crate::state_machine::Checker| matches!(checker.state, crate::state_machine::CheckerState::Error(Some(_))),
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
            move |checker: Box<crate::state_machine::Checker>| -> Result<(), wuffblob::error::WuffError> {
                let crate::state_machine::CheckerState::Error(Some(ref msg)) = checker.state else {
                    panic!("wrong state");
                };
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.any_not_repaired = true;
                });
                let _ui_lock = ctx.user_interaction.lock();
                println!("{:?}: {}", checker.path.to_osstring(), msg);
                Ok(())
            }
        },
        1000,
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |checker: &crate::state_machine::Checker| {
            matches!(
                checker.state,
                crate::state_machine::CheckerState::UserInteraction(_)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |checker: &mut crate::state_machine::Checker| {
                do_ui(&ctx, checker)
            }
        },
        1000,
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |checker: &crate::state_machine::Checker| {
            matches!(checker.state, crate::state_machine::CheckerState::Hash)
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |checker: Box<crate::state_machine::Checker>| {
                do_hash(std::sync::Arc::clone(&ctx), checker)
            }
        },
        1000,
        ctx.base_ctx.data_concurrency,
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |checker: &crate::state_machine::Checker| {
            matches!(
                checker.state,
                crate::state_machine::CheckerState::UpdateProperties(_)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |checker: Box<crate::state_machine::Checker>| {
                do_update_properties(std::sync::Arc::clone(&ctx), checker)
            }
        },
        1000,
        ctx.base_ctx.metadata_concurrency,
    );

    runner
        .run_async(
            &ctx.base_ctx,
            {
                let ctx: std::sync::Arc<crate::ctx::Ctx> =
                    std::sync::Arc::clone(&ctx);
                move |writer: tokio::sync::mpsc::Sender<
                    Box<crate::state_machine::Checker>,
                >| { feed(ctx, writer) }
            },
            1000,
        )
        .await?;

    let stats: crate::ctx::Stats = ctx.get_stats();
    println!(
        "{} files, {} bytes used",
        stats.files_found, stats.bytes_found
    );
    if stats.any_propupd_attempted && !ctx.preen {
        print!("\n***** FILE SYSTEM WAS MODIFIED *****\n");
    }
    if stats.any_not_repaired {
        println!("");
        Err("***** SOME FILES WERE NOT REPAIRED *****".into())
    } else {
        Ok(())
    }
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command =
        wuffblob::ctx::make_cmdline_parser("wuffblob-fsck")
            .arg(
                clap::Arg::new("preen")
                    .long("preen")
                    .short('p')
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                clap::Arg::new("yes")
                    .long("yes")
                    .short('y')
                    .action(clap::ArgAction::SetTrue),
            );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let ctx: std::sync::Arc<crate::ctx::Ctx> =
        std::sync::Arc::new(crate::ctx::Ctx::new(&cmdline_matches)?);
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
