// The downloader is written in such a way that the first error it runs into,
// it will stop. Any progress it has made so far will remain on the local disk,
// obviously, and if the download is retried later it will mean that much less
// work that needs to be retried. But we won't purposely press on after an
// error. We want the user to be aware, and fix it.
//
//   - The feeder is responsible for figuring out the remote_path, the
//     local_path, and the remote_metadata, then it passes it on to the
//     mkdir stage.
//   - The mkdir stage does two jobs: it makes directories that correspond
//     to directories in Azure; and it makes parent directories for things
//     that are files in Azure. For directories, they're done after that.
//     For files, they get passed on to the open stage.
//   - The open stage is simple: it just opens the file, for both reading
//     and writing. It might pass things on to the local "prefix hasher"
//     stage to get a "prefix hash", meaning hash the first X bytes of the
//     local file and then store the hasher ready to accept more data. Or it
//     might pass things on to the downloader stage for a "suffix download",
//     meaning download everything after the first X bytes (in the degenerate
//     case, X could be zero, indicating a full download).
//   - The prefix hasher stage feeds into the downloader stage. If the sizes
//     are equal and the hashes are totally different, it'll truncate the file
//     and ask the downloader for a "suffix download" starting from zero. If
//     the local file is shorter than the file in Azure, it'll ask the
//     downloader for a "suffix download" starting from however big the local
//     file is. (Or, if the sizes are equal and the hashes are the same,
//     then yay, that's a terminal state!)
//   - The downloader stage can do suffix downloads, and if it determines
//     after it has done a suffix download that it also needs to do a prefix
//     download as well, then it can do that. If it does a prefix download
//     then it needs to pass it off to the "suffix hasher" stage. Or if the
//     hash is correct and it doesn't need to do a prefix download, then,
//     yay, that's terminal.
//   - The suffix hasher stage just makes sure that the complete file has
//     the hash we expect.

mod ctx;
mod state_machine;

async fn feed(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    writer: tokio::sync::mpsc::Sender<Box<crate::state_machine::Downloader>>,
) -> Result<(), wuffblob::error::WuffError> {
    for (remote_path, local_path) in &ctx.to_download {
        assert!(remote_path.is_componentless() || remote_path.is_canonical());

        let mut filename_as_string: String =
            match remote_path.to_osstring().into_string() {
                Ok(s) => s,
                Err(_) => {
                    return Err(format!(
                        "{:?}: path is not valid unicode",
                        &remote_path
                    )
                    .into());
                }
            };

        if !remote_path.is_componentless() {
            let blob_client: azure_storage_blobs::prelude::BlobClient = ctx
                .base_ctx
                .azure_client
                .container_client
                .blob_client(filename_as_string.clone());
            match blob_client.get_properties().into_future().await {
                Ok(resp) => {
                    let is_dir: bool = wuffblob::util::blob_is_dir(&resp.blob);
                    if is_dir {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.files_found += 1;
                        });
                    } else {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.files_found += 1;
                            stats.bytes_found +=
                                resp.blob.properties.content_length;
                        });
                    }
                    writer
                        .send(Box::new(crate::state_machine::Downloader::new(
                            &ctx,
                            remote_path.clone(),
                            local_path.clone(),
                            resp.blob.properties,
                            is_dir,
                        )))
                        .await
                        .expect("reader died");
                    if !is_dir {
                        // it's a file, don't do the recursive list
                        continue;
                    }
                }
                Err(e) => {
                    let azure_core::error::ErrorKind::HttpResponse {
                        status: azure_core::StatusCode::NotFound,
                        ..
                    } = e.kind()
                    else {
                        return Err(e.into());
                    };
                }
            }
        }

        let mut list_blobs_builder =
            ctx.base_ctx.azure_client.container_client.list_blobs();
        if !remote_path.is_componentless() {
            filename_as_string.push('/');
            list_blobs_builder = list_blobs_builder.prefix(filename_as_string);
        }
        let mut contents = list_blobs_builder.into_stream();
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
                if let azure_storage_blobs::container::operations::BlobItem::Blob(blob) = blob_item
                {
                    let one_remote_path =
                        wuffblob::path::WuffPath::from_osstr(std::ffi::OsStr::new(&blob.name));
                    if !one_remote_path.is_canonical() {
                        return Err(format!("{}: path is not canonical", &remote_path).into());
                    }
                    let one_local_path =
                        one_remote_path.merge_into_os_path(&remote_path, &local_path)?;
                    // Here's the big payoff for us having consumed everything:
                    // we get to move blob.properties. Yay!
                    let is_dir: bool = wuffblob::util::blob_is_dir(&blob);
                    if is_dir {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.files_found += 1;
                        });
                    } else {
                        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                            stats.files_found += 1;
                            stats.bytes_found += blob.properties.content_length;
                        });
                    }
                    writer
                        .send(Box::new(crate::state_machine::Downloader::new(
                            &ctx,
                            one_remote_path,
                            one_local_path,
                            blob.properties,
                            is_dir,
                        )))
                        .await
                        .expect("reader died");
                }
            }
        }
    }
    ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
        stats.done_listing = true;
    });
    Ok(())
}

fn do_mkdir(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    downloader: &mut crate::state_machine::Downloader,
) {
    // I don't quite trust std::fs::create_dir_all(), so I'm implementing my
    // own "mkdir -p" style thing here.
    //
    // Quoting the source for FreeBSD's "mkdir -p", which, itself, is quoting
    // POSIX 1003.2:
    //
    //     For each dir operand that does not name an existing
    //     directory, effects equivalent to those caused by the
    //     following command shall occur:
    //
    //     mkdir -p -m $(umask -S),u+wx $(dirname dir) &&
    //        mkdir [-m mode] dir
    //
    // So basically that means, check it first. If it doesn't exist,
    // recurse down until you find something that does exist, and then
    // go back up mkdir'ing everything.
    //
    // What FreeBSD actually does is slightly different from that, it just
    // always starts from the beginning, replacing every '/' with a nul byte,
    // doing a mkdir, and then replacing the nul byte with a '/' again.
    //
    // In our context here, we're going to be doing this A LOT. Once per file.
    // So I think the optimization suggested by POSIX, where we check first,
    // is probably worthwhile.
    //
    // Somewhat unintuitively, FreeBSD does nothing to filter out "..", so
    // neither shall we.
    let path_to_mkdir: std::path::PathBuf =
        if let crate::state_machine::DownloaderState::Mkdir(
            ref path_to_mkdir,
        ) = downloader.state
        {
            path_to_mkdir.clone()
        } else {
            panic!("wrong state");
        };

    let mut depth: usize = 0;
    for component in path_to_mkdir.components() {
        if let std::path::Component::Prefix(_)
        | std::path::Component::RootDir = component
        {
            depth = 0;
        } else {
            depth += 1;
        }
    }
    // depth is now the number of things we can try. For example, if the path
    // were "/foo/bar/baz" then depth would be 3, and we would try:
    //   "/foo/bar/baz"
    //   "/foo/bar"
    //   "/foo"
    // We would not want to try "/". In fact, if depth is zero, then we just
    // have to assume we're okay and we shouldn't worry.
    //
    // The rest of this method assumes depth is nonzero.
    if depth == 0 {
        downloader.mkdir_succeeded(ctx);
        return;
    }

    let mut need_mkdir_idx: usize = {
        // Look for something that exists. When we find it, go back one,
        // to the last thing that *didn't* exist.
        let mut path: &std::path::Path = &path_to_mkdir;
        let mut i: usize = 0;
        loop {
            debug_assert!(i < depth);
            match std::fs::metadata(path) {
                Ok(metadata) => {
                    let file_type: std::fs::FileType = metadata.file_type();
                    if file_type.is_dir() {
                        if i == 0 {
                            downloader.mkdir_succeeded(ctx);
                            return;
                        } else {
                            // we found something that existed. Go back one,
                            // and that's what we need to start by mkdir'ing.
                            break i - 1;
                        }
                    } else if !file_type.is_symlink() {
                        downloader.mkdir_failed(
                            ctx,
                            &format!(
                                "{:?} is a {:?}, not a directory",
                                path, file_type
                            )
                            .into(),
                        );
                        return;
                    }
                    // if it's a symlink, that is a little weird and probably
                    // shouldn't have happened. But I can imagine some systems
                    // maybe doing that for a dangling symlink. Let's treat it
                    // the same as if it didn't exist.
                }
                Err(e) => {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        downloader.mkdir_failed(
                            ctx,
                            &format!("{:?}: {}", path, e).into(),
                        );
                        return;
                    }
                }
            }
            i += 1;
            if i < depth {
                path = path.parent().unwrap();
            } else {
                // We got all the way down. We can't take the parent again.
                // If we did take the parent again, we'd get down to something
                // like "/" which we definitely don't want to mkdir.
                //
                // So we just have to assume that we can start from where we're
                // at now -- or where we *were*, before we added 1 to i.
                break i - 1;
            }
        }
    };

    loop {
        let mut path: &std::path::Path = &path_to_mkdir;
        for _ in 0..need_mkdir_idx {
            path = path.parent().unwrap();
        }
        if let Err(e) = std::fs::create_dir(path) {
            if e.kind() != std::io::ErrorKind::AlreadyExists {
                downloader
                    .mkdir_failed(ctx, &format!("{:?}: {}", path, e).into());
                return;
            }
        }
        if need_mkdir_idx == 0 {
            break;
        } else {
            need_mkdir_idx -= 1;
        }
    }

    downloader.mkdir_succeeded(ctx);
}

fn do_open(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    downloader: &mut crate::state_machine::Downloader,
) {
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&downloader.local_path)
    {
        Ok(f) => {
            downloader.open_succeeded(ctx, f);
            if let crate::state_machine::DownloaderState::SuffixDownload(
                marker_pos,
                _,
            )
            | crate::state_machine::DownloaderState::PrefixHash(
                marker_pos,
            ) = downloader.state
            {
                // The assumption, which we'll go with until we find out
                // differently, is that from zero to 'marker_pos' is okay
                // (but will need to be hashed), and from 'marker_pos' to
                // the end of the file will need to be downloaded.
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.bytes_need_hashing_locally += marker_pos;
                    stats.bytes_can_reuse += marker_pos;
                    stats.bytes_need_download +=
                        downloader.remote_metadata.content_length - marker_pos;
                });
            }
        }
        Err(e) => {
            downloader.open_failed(ctx, &wuffblob::error::WuffError::from(e));
        }
    }
}

fn do_prefix_hash(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    buf: &mut Vec<u8>,
    downloader: &mut crate::state_machine::Downloader,
) {
    let _local_io_bound_operations_lock = ctx.local_io_bound_operations.lock();
    let crate::state_machine::DownloaderState::PrefixHash(marker_pos) =
        downloader.state
    else {
        panic!("wrong state");
    };
    buf.resize(
        wuffblob::util::io_block_size(
            downloader.local_metadata.as_ref().unwrap(),
        ),
        0u8,
    );
    let mut hasher: md5::Md5 = <md5::Md5 as md5::Digest>::new();
    let mut num_bytes_left: u64 = marker_pos;
    match std::io::Seek::seek(
        downloader.local_file.as_mut().unwrap(),
        std::io::SeekFrom::Start(0),
    ) {
        Ok(off) => {
            debug_assert!(off == 0);
        }
        Err(e) => {
            downloader
                .prefix_hash_failed(ctx, &wuffblob::error::WuffError::from(e));
            return;
        }
    }
    while num_bytes_left > 0 {
        if (buf.len() as u64) > num_bytes_left {
            buf.resize(num_bytes_left as usize, 0u8);
        }
        if let Err(e) = std::io::Read::read_exact(
            downloader.local_file.as_mut().unwrap(),
            buf.as_mut_slice(),
        ) {
            downloader
                .prefix_hash_failed(ctx, &wuffblob::error::WuffError::from(e));
            return;
        }
        <md5::Md5 as md5::Digest>::update(&mut hasher, &buf);
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.bytes_completed_hashing_locally += buf.len() as u64;
        });
        num_bytes_left -= buf.len() as u64;
    }
    downloader.prefix_hash_succeeded(ctx, hasher);
    if let crate::state_machine::DownloaderState::SuffixDownload(
        new_marker_pos,
        _,
    ) = downloader.state
    {
        // Has the plan changed? The new marker pos might be different
        // from the old one.
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.bytes_can_reuse -= marker_pos;
            stats.bytes_can_reuse += new_marker_pos;
            stats.bytes_need_download += marker_pos;
            stats.bytes_need_download -= new_marker_pos;
        });
    }
}

fn do_suffix_hash(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    buf: &mut Vec<u8>,
    downloader: &mut crate::state_machine::Downloader,
) {
    let _local_io_bound_operations_lock = ctx.local_io_bound_operations.lock();
    let (marker_pos, mut hasher) =
        if let crate::state_machine::DownloaderState::SuffixHash(
            marker_pos,
            ref hasher,
        ) = downloader.state
        {
            (marker_pos, hasher.clone())
        } else {
            panic!("wrong state");
        };

    let io_block_size: usize = wuffblob::util::io_block_size(
        downloader.local_metadata.as_ref().unwrap(),
    );

    match std::io::Seek::seek(
        downloader.local_file.as_mut().unwrap(),
        std::io::SeekFrom::Start(marker_pos),
    ) {
        Ok(off) => {
            debug_assert!(off == marker_pos);
        }
        Err(e) => {
            downloader
                .suffix_hash_failed(ctx, &wuffblob::error::WuffError::from(e));
            return;
        }
    }
    debug_assert!(marker_pos < downloader.remote_metadata.content_length);
    let mut num_bytes_left: u64 =
        downloader.remote_metadata.content_length - marker_pos;

    // First get ourselves to a block boundary
    if (marker_pos % (io_block_size as u64)) != 0 {
        let num_bytes_to_boundary: u64 = marker_pos
            - (marker_pos % (io_block_size as u64))
            + (io_block_size as u64);
        if num_bytes_to_boundary < num_bytes_left {
            buf.resize(num_bytes_to_boundary as usize, 0u8);
            if let Err(e) = std::io::Read::read_exact(
                downloader.local_file.as_mut().unwrap(),
                buf.as_mut_slice(),
            ) {
                downloader.suffix_hash_failed(
                    ctx,
                    &wuffblob::error::WuffError::from(e),
                );
                return;
            }
            <md5::Md5 as md5::Digest>::update(&mut hasher, &buf);
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.bytes_completed_hashing_locally += buf.len() as u64;
            });
            num_bytes_left -= buf.len() as u64;
        }
    }

    buf.resize(io_block_size, 0u8);
    while num_bytes_left > 0 {
        if (buf.len() as u64) > num_bytes_left {
            buf.resize(num_bytes_left as usize, 0u8);
        }
        if let Err(e) = std::io::Read::read_exact(
            downloader.local_file.as_mut().unwrap(),
            buf.as_mut_slice(),
        ) {
            downloader
                .suffix_hash_failed(ctx, &wuffblob::error::WuffError::from(e));
            return;
        }
        <md5::Md5 as md5::Digest>::update(&mut hasher, &buf);
        ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
            stats.bytes_completed_hashing_locally += buf.len() as u64;
        });
        num_bytes_left -= buf.len() as u64;
    }
    downloader.suffix_hash_succeeded(
        ctx,
        <md5::Md5 as md5::Digest>::finalize(hasher)
            .as_slice()
            .try_into()
            .unwrap(),
    );
}

async fn do_download(
    ctx: std::sync::Arc<crate::ctx::Ctx>,
    mut downloader: Box<crate::state_machine::Downloader>,
) -> Box<crate::state_machine::Downloader> {
    let (off_start, off_end, mut hasher) = match downloader.state {
        crate::state_machine::DownloaderState::PrefixDownload(marker_pos) => {
            (0, marker_pos, <md5::Md5 as md5::Digest>::new())
        }
        crate::state_machine::DownloaderState::SuffixDownload(
            marker_pos,
            ref hasher,
        ) => (
            marker_pos,
            downloader.remote_metadata.content_length,
            hasher.clone(),
        ),
        _ => {
            panic!("wrong state");
        }
    };

    match std::io::Seek::seek(
        downloader.local_file.as_mut().unwrap(),
        std::io::SeekFrom::Start(off_start),
    ) {
        Ok(off) => {
            debug_assert!(off == off_start);
        }
        Err(e) => {
            downloader
                .download_failed(&ctx, &wuffblob::error::WuffError::from(e));
            return downloader;
        }
    }

    // If this is a suffix download, truncate the file to where we currently
    // are. This is not necessary for correctness, but it will help if we're
    // interrupted before we finish, because when the user retries, it'll be
    // obvious where they should resume from.
    if off_end == downloader.remote_metadata.content_length {
        if let Err(e) =
            downloader.local_file.as_mut().unwrap().set_len(off_start)
        {
            downloader
                .download_failed(&ctx, &wuffblob::error::WuffError::from(e));
            return downloader;
        }
    }

    let filename_as_string: String =
        match downloader.remote_path.to_osstring().into_string() {
            Ok(s) => s,
            Err(_) => {
                downloader.download_failed(
                    &ctx,
                    &wuffblob::error::WuffError::from(
                        "path is not valid unicode",
                    ),
                );
                return downloader;
            }
        };

    let blob_client: azure_storage_blobs::prelude::BlobClient = ctx
        .base_ctx
        .azure_client
        .container_client
        .blob_client(filename_as_string);

    let mut chunks_stream = blob_client
        .get()
        .range(azure_core::request_options::Range::new(off_start, off_end))
        .into_stream();
    debug_assert!(off_start < off_end);
    let expected_len: u64 = off_end - off_start;
    let mut num_bytes_downloaded: u64 = 0;
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
                            num_bytes_downloaded += buf_len as u64;
                            if num_bytes_downloaded <= expected_len {
                                if let Err(e) = std::io::Write::write_all(
                                    downloader.local_file.as_mut().unwrap(),
                                    buf,
                                ) {
                                    downloader.download_failed(
                                        &ctx,
                                        &wuffblob::error::WuffError::from(e),
                                    );
                                    return downloader;
                                }
                                <md5::Md5 as md5::Digest>::update(
                                    &mut hasher,
                                    buf,
                                );
                                ctx.mutate_stats(
                                    |stats: &mut crate::ctx::Stats| {
                                        stats.bytes_downloaded +=
                                            buf_len as u64;
                                    },
                                );
                            }
                        }
                        Err(err) => {
                            downloader.download_failed(
                                &ctx,
                                &wuffblob::error::WuffError::from(err),
                            );
                            return downloader;
                        }
                    }
                }
            }
            Err(ref err) => {
                downloader.download_failed(
                    &ctx,
                    &wuffblob::error::WuffError::from(err),
                );
                return downloader;
            }
        }
    }
    if num_bytes_downloaded != expected_len {
        downloader.download_failed(
            &ctx,
            &wuffblob::error::WuffError::from(format!(
                "Expected {} bytes, got {} instead",
                expected_len, num_bytes_downloaded
            )),
        );
        return downloader;
    }
    match downloader.state {
        crate::state_machine::DownloaderState::PrefixDownload(_) => {
            downloader.prefix_download_succeeded(&ctx, hasher);
        }
        crate::state_machine::DownloaderState::SuffixDownload(_, _) => {
            downloader.suffix_download_succeeded(
                &ctx,
                <md5::Md5 as md5::Digest>::finalize(hasher)
                    .as_slice()
                    .try_into()
                    .unwrap(),
            );
            if let crate::state_machine::DownloaderState::PrefixDownload(
                new_marker_pos,
            ) = downloader.state
            {
                ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                    stats.bytes_need_hashing_locally +=
                        downloader.remote_metadata.content_length
                            - new_marker_pos;
                    stats.bytes_can_reuse -= off_start;
                    stats.bytes_need_download += new_marker_pos;
                });
            }
        }
        _ => {
            panic!("wrong state");
        }
    };

    downloader
}

fn do_finalize(
    ctx: &std::sync::Arc<crate::ctx::Ctx>,
    downloader: &mut crate::state_machine::Downloader,
) {
    let _local_io_bound_operations_lock = ctx.local_io_bound_operations.lock();
    let desired_len: u64 = downloader.remote_metadata.content_length;
    if let Err(e) =
        downloader.local_file.as_mut().unwrap().set_len(desired_len)
    {
        downloader.finalize_failed(ctx, &wuffblob::error::WuffError::from(e));
        return;
    }
    if let Err(e) = downloader.local_file.as_mut().unwrap().sync_all() {
        downloader.finalize_failed(ctx, &wuffblob::error::WuffError::from(e));
        return;
    }
    downloader.finalize_succeeded(ctx);
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

    let mut runner: wuffblob::runner::Runner<
        crate::state_machine::Downloader,
    > = wuffblob::runner::Runner::new();

    runner.handle_terminal(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(downloader.state, crate::state_machine::DownloaderState::Ok)
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
        move |_downloader: Box<crate::state_machine::Downloader>| -> Result<(), wuffblob::error::WuffError> {
            ctx.mutate_stats(|stats: &mut crate::ctx::Stats| {
                stats.files_done += 1;
            });
            Ok(())
        }},
        1000
    );

    runner.handle_terminal(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::Error(_)
            )
        },
        |downloader: Box<crate::state_machine::Downloader>| -> Result<(), wuffblob::error::WuffError> {
            if let crate::state_machine::DownloaderState::Error(e) = downloader.state {
                Err(e.into())
            } else {
                panic!("wrong state");
            }
        },
        1000
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::Mkdir(_)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |downloader: &mut crate::state_machine::Downloader| {
                do_mkdir(&ctx, downloader)
            }
        },
        1000,
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::Open
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |downloader: &mut crate::state_machine::Downloader| {
                do_open(&ctx, downloader)
            }
        },
        1000,
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::PrefixHash(_)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            let mut buf: Vec<u8> = Vec::new();
            move |downloader: &mut crate::state_machine::Downloader| {
                do_prefix_hash(&ctx, &mut buf, downloader)
            }
        },
        1, // each one holds a file descriptor! can't have thousands!
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::SuffixHash(_, _)
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            let mut buf: Vec<u8> = Vec::new();
            move |downloader: &mut crate::state_machine::Downloader| {
                do_suffix_hash(&ctx, &mut buf, downloader)
            }
        },
        1, // each one holds a file descriptor! can't have thousands!
    );

    runner.handle_nonterminal_async(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::PrefixDownload(_)
                    | crate::state_machine::DownloaderState::SuffixDownload(
                        _,
                        _
                    )
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |downloader: Box<crate::state_machine::Downloader>| {
                do_download(std::sync::Arc::clone(&ctx), downloader)
            }
        },
        1, // each one holds a file descriptor! can't have thousands!
        ctx.base_ctx.data_concurrency,
    );

    runner.handle_nonterminal_blocking(
        &ctx.base_ctx,
        |downloader: &crate::state_machine::Downloader| {
            matches!(
                downloader.state,
                crate::state_machine::DownloaderState::Finalize
            )
        },
        {
            let ctx: std::sync::Arc<crate::ctx::Ctx> =
                std::sync::Arc::clone(&ctx);
            move |downloader: &mut crate::state_machine::Downloader| {
                do_finalize(&ctx, downloader)
            }
        },
        1, // each one holds a file descriptor! can't have thousands!
    );

    // We run the feeder with a queue size of 10,000, to make up for the fact
    // that we run most of the other nonterminal states with a queue size of 1.
    runner
        .run_async(
            &ctx.base_ctx,
            {
                let ctx: std::sync::Arc<crate::ctx::Ctx> =
                    std::sync::Arc::clone(&ctx);
                move |writer: tokio::sync::mpsc::Sender<
                    Box<crate::state_machine::Downloader>,
                >| { feed(ctx, writer) }
            },
            10000,
        )
        .await
}

fn main() -> Result<(), wuffblob::error::WuffError> {
    let cmdline_parser: clap::Command =
        wuffblob::ctx::make_cmdline_parser("wuffblob-download")
            .arg(
                clap::Arg::new("paths")
                    .value_parser(clap::value_parser!(
                        wuffblob::path::WuffPath
                    ))
                    .action(clap::ArgAction::Append),
            )
            .arg(
                clap::Arg::new("download_as")
                    .long("download-as")
                    .value_parser(clap::value_parser!(std::path::PathBuf))
                    .action(clap::ArgAction::Set),
            );
    let cmdline_matches: clap::ArgMatches = cmdline_parser.get_matches();

    let mut raw_to_download: Vec<wuffblob::path::WuffPath> = Vec::new();
    if let Some(raw_paths) =
        cmdline_matches.get_many::<wuffblob::path::WuffPath>("paths")
    {
        for raw_path in raw_paths {
            if let Some(path) =
                raw_path.clone().canonicalize_or_componentless()
            {
                raw_to_download.push(path);
            } else {
                return Err(
                    format!("{:?}: invalid remote path", raw_path).into()
                );
            }
        }
    };
    let mut to_download: Vec<(wuffblob::path::WuffPath, std::path::PathBuf)> =
        Vec::with_capacity(raw_to_download.len());
    if let Some(download_as) =
        cmdline_matches.get_one::<std::path::PathBuf>("download_as")
    {
        if raw_to_download.is_empty() {
            to_download
                .push((wuffblob::path::WuffPath::new(), download_as.clone()));
        } else if raw_to_download.len() == 1 {
            to_download
                .push((raw_to_download.pop().unwrap(), download_as.clone()));
        } else {
            return Err("If download-as is specified, there must be at most one remote path specified to download".into());
        }
    } else if raw_to_download.is_empty() {
        to_download.push((wuffblob::path::WuffPath::new(), ".".into()));
    } else {
        for path in raw_to_download {
            let download_as: std::path::PathBuf = (&path).into();
            to_download.push((path, download_as));
        }

        // Checking for overlap in the remote paths will catch some possible
        // cases of overlap, but not all of them. We will still have to check
        // the st_dev/st_ino in all the files we open, to make sure we don't
        // have the same file open twice under two different names.
        //
        // But it's easy to check for overlap in the remote paths, and it
        // might catch some of the simpler cases, like "derp, I told it to
        // download both foo/ and foo/bar/"
        let mut paths: Vec<&wuffblob::path::WuffPath> =
            Vec::with_capacity(to_download.len());
        for (path, _) in &to_download {
            paths.push(path);
        }
        wuffblob::path::WuffPath::check_for_overlap(paths)?;
    }

    let ctx: std::sync::Arc<crate::ctx::Ctx> = std::sync::Arc::new(
        crate::ctx::Ctx::new(&cmdline_matches, to_download)?,
    );
    ctx.base_ctx
        .run_async_main(async_main(std::sync::Arc::clone(&ctx)))
}
