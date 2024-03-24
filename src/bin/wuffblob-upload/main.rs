// The uploader is written in such a way that the first error it runs into,
// it will stop. Any progress it has made so far will remain in the cloud,
// obviously, and if the upload is retried later it will mean that much less
// work that needs to be retried. But we won't purposely press on after an
// error. We want the user to be aware, and fix it.
//
//   - The feeder is responsible for figuring out the local_path, the
//     remote_path, and the local_metadata, then it passes it on to the
//     metadata stage.
//   - The metadata stage always starts with a "get" in Azure. If it
//     determines that all we need to do is make an empty directory, it'll
//     take care of that itself, rather than farming that off to some other
//     stage. It feeds into either the uploader stage or the local-hasher
//     stage.
//   - The local hasher stage feeds into the uploader stage.
//   - The uploader stage is also responsible for deleting the old blob in
//     Azure before it starts the upload, if that is needed. Yes that is a
//     metadata operation, yes it would be slightly more efficient to let
//     the metadata stage handle that part, but that would introduce a cycle
//     in the graph.
//   - After it is done uploading, if the user has requested verification, it
//     feeds into the verification stage.

mod ctx;
mod state_machine;

fn feed(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    writer: tokio::sync::mpsc::Sender<Box<crate::state_machine::Uploader>>,
) -> Result<(), wuffblob::error::WuffError> {
    // The rule is: you don't add stuff to the LIFO unless you have verified
    // that it is actually a bona-fide directory, and not just a symlink that
    // points at a directory.
    //
    // Exception: the very first entries we are seeded with from the ctx, may
    // be symlinks that point at directories.
    let mut lifo: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(ctx.to_upload.len());

    for (local_path, remote_path) in &ctx.to_upload {
        let maybe_metadata: Result<std::fs::Metadata, std::io::Error> =
            std::fs::metadata(local_path);
        if let Err(e) = maybe_metadata {
            return Err(format!("{:?}: {}", local_path, e).into());
        }
        let metadata: std::fs::Metadata = maybe_metadata.unwrap();
        let file_type: std::fs::FileType = metadata.file_type();
        if file_type.is_file() {
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_found += 1;
                stats.bytes_found += metadata.len();
            });
            writer
                .blocking_send(Box::new(crate::state_machine::Uploader::new(
                    &ctx,
                    local_path.clone(),
                    remote_path.clone(),
                    metadata,
                )))
                .expect("reader died");
        } else if file_type.is_dir() {
            lifo.push((local_path.clone(), remote_path.clone()));
        } else {
            return Err(format!(
                "{:?}: cannot upload {:?}",
                local_path, file_type
            )
            .into());
        }
    }

    while let Some((local_path, remote_path)) = lifo.pop() {
        let maybe_dir_entries: Result<std::fs::ReadDir, std::io::Error> =
            std::fs::read_dir(&local_path);
        if let Err(e) = maybe_dir_entries {
            return Err(format!("{:?}: {}", local_path, e).into());
        }
        let dir_entries: std::fs::ReadDir = maybe_dir_entries.unwrap();
        let mut seen_any: bool = false;
        for maybe_dir_entry in dir_entries {
            seen_any = true;
            if let Err(e) = maybe_dir_entry {
                return Err(format!("{:?}: {}", local_path, e).into());
            }
            let dir_entry: std::fs::DirEntry = maybe_dir_entry.unwrap();
            let new_basename: std::ffi::OsString = dir_entry.file_name();
            let new_local_path: std::path::PathBuf =
                local_path.join(&new_basename);
            let mut new_remote_path: wuffblob::path::WuffPath =
                remote_path.clone();
            new_remote_path.push(&new_basename);
            let maybe_orig_file_type: Result<
                std::fs::FileType,
                std::io::Error,
            > = dir_entry.file_type();
            if let Err(e) = maybe_orig_file_type {
                return Err(format!("{:?}: {}", new_local_path, e).into());
            }
            let orig_file_type: std::fs::FileType =
                maybe_orig_file_type.unwrap();
            let maybe_new_metadata: Result<std::fs::Metadata, std::io::Error> =
                std::fs::metadata(&new_local_path);
            if let Err(e) = maybe_new_metadata {
                return Err(format!("{:?}: {}", new_local_path, e).into());
            }
            let new_metadata: std::fs::Metadata = maybe_new_metadata.unwrap();
            // We mostly trust the metadata as far as what kind of thing this
            // is. But we do check one thing, which is we definitely want to
            // kick it out if it's a symlink that is pointing to a directory.
            let file_type: std::fs::FileType = new_metadata.file_type();
            if file_type.is_file() {
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.files_found += 1;
                    stats.bytes_found += new_metadata.len();
                });
                writer
                    .blocking_send(Box::new(
                        crate::state_machine::Uploader::new(
                            &ctx,
                            new_local_path,
                            new_remote_path,
                            new_metadata,
                        ),
                    ))
                    .expect("reader died");
            } else if file_type.is_dir() {
                if orig_file_type.is_symlink() {
                    return Err(format!(
                        "{:?}: cannot follow symbolic link to directory",
                        new_local_path
                    )
                    .into());
                } else {
                    lifo.push((new_local_path, new_remote_path));
                }
            } else {
                return Err(format!(
                    "{:?}: cannot upload {:?}",
                    new_local_path, file_type
                )
                .into());
            }
        }
        if !seen_any {
            let maybe_metadata: Result<std::fs::Metadata, std::io::Error> =
                std::fs::metadata(&local_path);
            if let Err(e) = maybe_metadata {
                return Err(format!("{:?}: {}", local_path, e).into());
            }
            let metadata: std::fs::Metadata = maybe_metadata.unwrap();
            // It had better be a directory...!
            let file_type: std::fs::FileType = metadata.file_type();
            if file_type.is_dir() {
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.files_found += 1;
                });
                writer
                    .blocking_send(Box::new(
                        crate::state_machine::Uploader::new(
                            &ctx,
                            local_path,
                            remote_path,
                            metadata,
                        ),
                    ))
                    .expect("reader died");
            } else {
                return Err(format!(
                    "{:?}: expecting directory, found {:?}",
                    local_path, file_type
                )
                .into());
            }
        }
    }
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.done_listing = true;
    });
    Ok(())
}

async fn do_metadata(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut uploader: Box<crate::state_machine::Uploader>,
) -> Box<crate::state_machine::Uploader> {
    let filename_as_string: String =
        match uploader.remote_path.to_osstring().into_string() {
            Ok(s) => s,
            Err(_) => {
                match uploader.state {
                    crate::state_machine::UploaderState::GetRemoteMetadata => {
                        uploader.get_remote_metadata_failed(
                            &ctx,
                            wuffblob::error::WuffError::from(
                                "path is not valid unicode",
                            ),
                        );
                    }
                    crate::state_machine::UploaderState::Mkdir => {
                        uploader.mkdir_failed(
                            &ctx,
                            wuffblob::error::WuffError::from(
                                "path is not valid unicode",
                            ),
                        );
                    }
                    _ => {
                        panic!("wrong state");
                    }
                }
                return uploader;
            }
        };

    match uploader.state {
        crate::state_machine::UploaderState::GetRemoteMetadata => {
            let blob_client: azure_storage_blobs::prelude::BlobClient = ctx
                .base_ctx
                .azure_client
                .container_client
                .blob_client(filename_as_string);
            match blob_client.get_properties().into_future().await {
                Ok(resp) => {
                    uploader
                        .provide_remote_metadata(&ctx, resp.blob.properties);
                }
                Err(e) => {
                    if let azure_core::error::ErrorKind::HttpResponse {
                        status: azure_core::StatusCode::NotFound,
                        ..
                    } = e.kind()
                    {
                        uploader.there_is_no_remote_metadata(&ctx);
                    } else {
                        uploader.get_remote_metadata_failed(
                            &ctx,
                            wuffblob::error::WuffError::from(e),
                        );
                    }
                }
            }
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_queried_in_cloud += 1;
            });

            match uploader.state {
                crate::state_machine::UploaderState::Hash => {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.files_need_hashing_locally += 1;
                        stats.bytes_need_hashing_locally +=
                            uploader.local_metadata.len();
                    });
                }
                crate::state_machine::UploaderState::Upload(_) => {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.files_need_upload += 1;
                        stats.bytes_need_upload +=
                            uploader.local_metadata.len();
                    });
                }
                crate::state_machine::UploaderState::Mkdir => {
                    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                        stats.files_need_upload += 1;
                    });
                }
                _ => {}
            }
        }
        crate::state_machine::UploaderState::Mkdir => {
            let dir_client: azure_storage_datalake::clients::DirectoryClient =
                ctx.base_ctx
                    .azure_client
                    .data_lake_client
                    .get_directory_client(filename_as_string);

            // Making a directory is an entirely best-effort operation. Some
            // types of Azure storage account don't even support directories.
            // So we try our best, but if we get an error there's nothing we
            // can do.
            let _ = dir_client
                .create_if_not_exists()
                .resource(azure_storage_datalake::request_options::ResourceType::Directory)
                .into_future()
                .await;
            // We always tell the uploader that it worked. What else can we do?
            uploader.mkdir_succeeded(&ctx);
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_uploaded += 1;
                stats.files_verified += 1;
            });
        }
        _ => {
            panic!("wrong state");
        }
    }
    uploader
}

fn do_hash(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    buf: &mut Vec<u8>,
    uploader: &mut crate::state_machine::Uploader,
) {
    let mut f: std::fs::File = {
        match std::fs::File::open(&uploader.local_path) {
            Ok(f) => f,
            Err(e) => {
                uploader.hash_failed(ctx, wuffblob::error::WuffError::from(e));
                return;
            }
        }
    };
    buf.resize(wuffblob::util::io_block_size(&uploader.local_metadata), 0u8);
    let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
    let mut num_bytes_left: u64 = uploader.local_metadata.len();
    while num_bytes_left > 0 {
        if (buf.len() as u64) > num_bytes_left {
            buf.resize(num_bytes_left as usize, 0u8);
        }
        if let Err(e) = std::io::Read::read_exact(&mut f, buf.as_mut_slice()) {
            uploader.hash_failed(ctx, wuffblob::error::WuffError::from(e));
            return;
        }
        <md5::Md5 as md5::Digest>::update(&mut hasher, &buf);
        num_bytes_left -= buf.len() as u64;
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.bytes_completed_hashing_locally += buf.len() as u64;
        });
    }
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.files_completed_hashing_locally += 1;
    });
    uploader.provide_hash(
        ctx,
        <md5::Md5 as md5::Digest>::finalize(hasher)
            .as_slice()
            .try_into()
            .unwrap(),
    );

    match uploader.state {
        crate::state_machine::UploaderState::Ok => {
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_can_reuse += 1;
                stats.bytes_can_reuse += uploader.local_metadata.len();
            });
        }

        crate::state_machine::UploaderState::Upload(_) => {
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_need_upload += 1;
                stats.bytes_need_upload += uploader.local_metadata.len();
            });
        }
        _ => {}
    }
}

async fn do_upload(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut uploader: Box<crate::state_machine::Uploader>,
) -> Box<crate::state_machine::Uploader> {
    let filename_as_string: String =
        match uploader.remote_path.to_osstring().into_string() {
            Ok(s) => s,
            Err(_) => {
                uploader.upload_failed(
                    &ctx,
                    wuffblob::error::WuffError::from(
                        "path is not valid unicode",
                    ),
                );
                return uploader;
            }
        };

    let blob_client: azure_storage_blobs::prelude::BlobClient = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(filename_as_string);

    let mark_progress: std::sync::Arc<dyn Fn(u64) + Send + Sync> =
        std::sync::Arc::new({
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            let progress: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(0);
            move |val: u64| {
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.bytes_uploaded -= progress
                        .swap(val, std::sync::atomic::Ordering::Relaxed);
                    stats.bytes_uploaded += val;
                });
            }
        });

    // Make sure we can open our local file, before we delete the one in Azure
    let f: wuffblob::azure::FileForUpload =
        match std::fs::File::open(&uploader.local_path) {
            Ok(f) => wuffblob::azure::FileForUpload::new(
                f,
                &uploader.local_metadata,
                std::sync::Arc::clone(&mark_progress),
            ),
            Err(e) => {
                uploader
                    .upload_failed(&ctx, wuffblob::error::WuffError::from(e));
                return uploader;
            }
        };

    if let crate::state_machine::UploaderState::Upload(true) = uploader.state {
        if let Err(e) = blob_client.delete().into_future().await {
            uploader.upload_failed(&ctx, wuffblob::error::WuffError::from(e));
            return uploader;
        }
    }

    let num_blob_blocks: u16 = f.num_blob_blocks();
    let mut block_ids: Vec<azure_storage_blobs::prelude::BlobBlockType> =
        Vec::with_capacity(num_blob_blocks as usize);
    for block_num in 0..num_blob_blocks {
        let chunk: Box<dyn azure_core::SeekableStream> =
            Box::new(f.get_chunk_for_upload(block_num));

        // for some reason you can make a BlockId from a vector but not from
        // an array directly. which is no problem, it's just odd. We can make
        // a vector.
        let block_num_as_bytes: Vec<u8> = block_num.to_be_bytes().into();

        let block_id: azure_storage_blobs::prelude::BlockId =
            azure_storage_blobs::prelude::BlockId::new(block_num_as_bytes);
        block_ids.push(
            azure_storage_blobs::blob::BlobBlockType::new_uncommitted(
                block_id.clone(),
            ),
        );

        if let Err(e) =
            blob_client.put_block(block_id, chunk).into_future().await
        {
            uploader.upload_failed(&ctx, wuffblob::error::WuffError::from(e));
            return uploader;
        }
    }
    let hash: [u8; 16] = match f.get_hash() {
        Ok(hash) => hash,
        Err(e) => {
            uploader.upload_failed(&ctx, e);
            return uploader;
        }
    };
    if let Err(e) = blob_client
        .put_block_list(azure_storage_blobs::blob::BlockList {
            blocks: block_ids,
        })
        .content_md5(hash)
        .content_type(uploader.desired_content_type)
        .into_future()
        .await
    {
        uploader.upload_failed(&ctx, wuffblob::error::WuffError::from(e));
        return uploader;
    }
    uploader.upload_succeeded(&ctx, hash);
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.files_uploaded += 1;
    });
    mark_progress(uploader.local_metadata.len());

    uploader
}

async fn do_verify(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut uploader: Box<crate::state_machine::Uploader>,
) -> Box<crate::state_machine::Uploader> {
    let filename_as_string: String =
        match uploader.remote_path.to_osstring().into_string() {
            Ok(s) => s,
            Err(_) => {
                uploader.verify_failed(
                    &ctx,
                    wuffblob::error::WuffError::from(
                        "path is not valid unicode",
                    ),
                );
                return uploader;
            }
        };

    let blob_client: azure_storage_blobs::prelude::BlobClient = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(filename_as_string);

    let mut chunks_stream = blob_client.get().into_stream();
    let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
    let expected_len: u64 = uploader.local_metadata.len();
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
                                    stats.bytes_verified += buf_len as u64;
                                },
                            );
                        }
                        Err(err) => {
                            uploader.verify_failed(
                                &ctx,
                                wuffblob::error::WuffError::from(err),
                            );
                            return uploader;
                        }
                    }
                }
            }
            Err(ref err) => {
                uploader.verify_failed(
                    &ctx,
                    wuffblob::error::WuffError::from(err),
                );
                return uploader;
            }
        }
    }
    if num_bytes_hashed != expected_len {
        uploader.verify_failed(
            &ctx,
            wuffblob::error::WuffError::from(format!(
                "Expected {} bytes, got {} instead",
                expected_len, num_bytes_hashed
            )),
        );
        return uploader;
    }
    uploader.verify_succeeded(
        &ctx,
        <md5::Md5 as md5::Digest>::finalize(hasher)
            .as_slice()
            .try_into()
            .unwrap(),
    );

    if let crate::state_machine::UploaderState::Ok = uploader.state {
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.files_verified += 1;
        });
    }

    uploader
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

    let mut runner: wuffblob::runner::Runner<crate::state_machine::Uploader> =
        wuffblob::runner::Runner::new();

    runner.handle_terminal(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(uploader.state, crate::state_machine::UploaderState::Ok)
        },
        |_uploader: Box<crate::state_machine::Uploader>| -> Result<(), wuffblob::error::WuffError> {
            Ok(())
        },
        1000,
    );

    runner.handle_terminal(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(
                uploader.state,
                crate::state_machine::UploaderState::Error(_)
            )
        },
        |uploader: Box<crate::state_machine::Uploader>| -> Result<(), wuffblob::error::WuffError> {
            if let crate::state_machine::UploaderState::Error(e) = uploader.state {
                Err(e.into())
            } else {
                panic!("wrong state");
            }
        },
        1000,
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(
                uploader.state,
                crate::state_machine::UploaderState::GetRemoteMetadata
                    | crate::state_machine::UploaderState::Mkdir
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |uploader: Box<crate::state_machine::Uploader>| {
                do_metadata(std::sync::Arc::clone(&ctx), uploader)
            }
        },
        1000,
        ctx.base_ctx.metadata_concurrency,
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(uploader.state, crate::state_machine::UploaderState::Hash)
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            let mut buf: Vec<u8> = Vec::new();
            move |uploader: &mut crate::state_machine::Uploader| {
                do_hash(&ctx, &mut buf, uploader)
            }
        },
        1000,
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(
                uploader.state,
                crate::state_machine::UploaderState::Upload(_)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |uploader: Box<crate::state_machine::Uploader>| {
                do_upload(std::sync::Arc::clone(&ctx), uploader)
            }
        },
        1000,
        ctx.base_ctx.data_concurrency,
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |uploader: &crate::state_machine::Uploader| {
            matches!(
                uploader.state,
                crate::state_machine::UploaderState::Verify
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |uploader: Box<crate::state_machine::Uploader>| {
                do_verify(std::sync::Arc::clone(&ctx), uploader)
            }
        },
        1000,
        ctx.base_ctx.data_concurrency,
    );

    runner
        .run_blocking(
            &ctx.base_ctx,
            {
                let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::clone(&ctx);
                move |writer: tokio::sync::mpsc::Sender<Box<crate::state_machine::Uploader>>| -> Result<(), wuffblob::error::WuffError> { feed(&ctx, writer) }
            },
            1000,
        )
        .await
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command =
        wuffblob::ctx::make_cmdline_parser("wuffblob-upload")
            .arg(
                clap::Arg::new("paths")
                    .value_parser(clap::value_parser!(std::path::PathBuf))
                    .action(clap::ArgAction::Append),
            )
            .arg(
                clap::Arg::new("upload_as")
                    .long("upload-as")
                    .value_parser(clap::value_parser!(
                        wuffblob::path::WuffPath
                    ))
                    .action(clap::ArgAction::Set),
            )
            .arg(
                clap::Arg::new("force")
                    .long("force")
                    .short('f')
                    .action(clap::ArgAction::SetTrue),
            )
            .arg(
                clap::Arg::new("verify")
                    .long("verify")
                    .action(clap::ArgAction::SetTrue),
            );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let raw_to_upload: Vec<&std::path::PathBuf> = if let Some(paths) =
        cmdline_matches.get_many::<std::path::PathBuf>("paths")
    {
        Vec::from_iter(paths)
    } else {
        Vec::new()
    };
    let mut to_upload: Vec<(std::path::PathBuf, wuffblob::path::WuffPath)> =
        Vec::with_capacity(raw_to_upload.len());
    if let Some(raw_upload_as) =
        cmdline_matches.get_one::<wuffblob::path::WuffPath>("upload_as")
    {
        if let Some(upload_as) =
            raw_upload_as.clone().canonicalize_or_componentless()
        {
            if raw_to_upload.is_empty() {
                to_upload.push((".".into(), upload_as));
            } else if raw_to_upload.len() == 1 {
                to_upload.push((raw_to_upload[0].clone(), upload_as));
            } else {
                return Err("If upload-as is specified, there must be at most one local path specified to upload".into());
            }
        } else {
            return Err(format!(
                "Upload-as path given as {} which is not canonicalizable",
                raw_upload_as
            )
            .into());
        }
    } else if raw_to_upload.is_empty() {
        to_upload.push((".".into(), wuffblob::path::WuffPath::new()));
    } else {
        for path in raw_to_upload {
            if let Some(upload_as) =
                TryInto::<wuffblob::path::WuffPath>::try_into(path.as_path())?
                    .canonicalize_or_componentless()
            {
                to_upload.push((path.clone(), upload_as));
            } else {
                return Err(format!(
                    "{:?} is not canonicalizable (try --upload-as)",
                    path
                )
                .into());
            }
        }

        // make sure there's no overlap on the destination side. It's OK to
        // have overlap on the source side since that just means the same
        // data is going to two different locations. But overlap on the
        // destination side means they conflict with each other.
        let mut paths: Vec<&wuffblob::path::WuffPath> =
            Vec::with_capacity(to_upload.len());
        for (_, path) in &to_upload {
            paths.push(path);
        }
        wuffblob::path::WuffPath::check_for_overlap(paths)?;
    }

    let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::new(
        crate::ctx::Ctx::new(&cmdline_matches, to_upload)?,
    );
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
