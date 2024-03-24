#[derive(Debug, Clone)]
pub enum DownloaderState {
    // Success! Terminal state.
    Ok,

    // Error. Terminal state.
    Error(String),

    // Please mkdir the indicated path, which could be me, or my parent.
    Mkdir(std::path::PathBuf),

    // Please open me, creating if necessary.
    Open,

    // Please make a new hasher, and hash the first N bytes of the file.
    PrefixHash(u64),

    // Please download the file starting from offset N bytes.
    SuffixDownload(u64, md5::Md5),

    // Please make a new hasher, and download the first N bytes of the file,
    // leaving all the data after N bytes intact.
    PrefixDownload(u64),

    // Please hash the file starting from offset N bytes.
    SuffixHash(u64, md5::Md5),

    // Please truncate me to the proper length and fsync me.
    Finalize,
}

#[derive(Debug)]
pub struct Downloader {
    pub remote_path: wuffblob::path::WuffPath,
    pub local_path: std::path::PathBuf,
    pub is_dir: bool,
    pub state: DownloaderState,
    pub remote_metadata: azure_storage_blobs::blob::BlobProperties,
    pub local_file: Option<std::fs::File>,
    pub local_metadata: Option<std::fs::Metadata>,
    dev_ino: Option<crate::ctx::DevIno>,
}

impl Downloader {
    pub fn new(
        _ctx: &std::sync::Arc<crate::ctx::Ctx>,
        remote_path: wuffblob::path::WuffPath,
        local_path: std::path::PathBuf,
        remote_metadata: azure_storage_blobs::blob::BlobProperties,
        is_dir: bool,
    ) -> Downloader {
        // I don't find the behavior of std::path::Path::parent() useful here.
        // Basically I'm trying to figure out if the path is something like:
        //
        //   "foo/bar/baz"
        //
        // where both "foo/bar" and "foo/bar/baz" are something you might
        // reasonably try to mkdir or write a file to; or something like:
        //
        //   "foo"
        //
        // where "foo" is something you might reasonably try to mkdir or write
        // a file to, but "" is not; or something like:
        //
        //   ""
        //   or
        //   "/"
        //
        // where either it exists or it doesn't, but you probably shouldn't
        // be trying to mkdir it, and you definitely shouldn't try to stick an
        // actual file there.
        //
        // Note that FreeBSD "mkdir -p" is actually totally fine with paths
        // that contain "." and "..", so we will emulate that behavior.
        let mut self_ok_for_file: bool = false;
        let mut self_reasonable_to_mkdir: bool = false;
        let mut parent_reasonable_to_mkdir: bool = false;
        for component in local_path.components() {
            parent_reasonable_to_mkdir = self_reasonable_to_mkdir;
            match component {
                std::path::Component::Normal(_) => {
                    self_ok_for_file = true;
                    self_reasonable_to_mkdir = true;
                }
                std::path::Component::Prefix(_)
                | std::path::Component::RootDir => {
                    self_ok_for_file = false;
                    self_reasonable_to_mkdir = false;
                    parent_reasonable_to_mkdir = false;
                }
                std::path::Component::CurDir
                | std::path::Component::ParentDir => {
                    self_ok_for_file = false;
                    self_reasonable_to_mkdir = true; // see note above
                }
            }
        }

        let state: DownloaderState = if is_dir {
            if self_reasonable_to_mkdir {
                DownloaderState::Mkdir(local_path.clone())
            } else {
                DownloaderState::Ok
            }
        } else {
            if !self_ok_for_file {
                DownloaderState::Error(format!(
                    "{:?}: cannot put a file here",
                    &local_path
                ))
            } else if parent_reasonable_to_mkdir {
                DownloaderState::Mkdir(local_path.parent().unwrap().into())
            } else {
                DownloaderState::Open
            }
        };
        Downloader {
            remote_path: remote_path,
            local_path: local_path,
            is_dir: is_dir,
            state: state,
            remote_metadata: remote_metadata,
            local_file: None,
            local_metadata: None,
            dev_ino: None,
        }
    }

    pub fn mkdir_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, DownloaderState::Mkdir(_)) {
            panic!(
                "State is {:?}, expected DownloaderState::Mkdir",
                &self.state
            );
        }

        self.set_state_to_local_error(ctx, err);
    }

    pub fn mkdir_succeeded(&mut self, _ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if !matches!(self.state, DownloaderState::Mkdir(_)) {
            panic!(
                "State is {:?}, expected DownloaderState::Mkdir",
                &self.state
            );
        }

        self.state = if self.is_dir {
            DownloaderState::Ok
        } else {
            DownloaderState::Open
        };
    }

    pub fn open_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, DownloaderState::Open) {
            panic!(
                "State is {:?}, expected DownloaderState::Open",
                &self.state
            );
        }

        self.set_state_to_local_error(ctx, err);
    }

    pub fn open_succeeded(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        file: std::fs::File,
    ) {
        if !matches!(self.state, DownloaderState::Open) {
            panic!(
                "State is {:?}, expected DownloaderState::Open",
                &self.state
            );
        }

        match file.metadata() {
            Ok(metadata) => {
                if !metadata.file_type().is_file() {
                    self.set_state_to_local_error(
                        ctx,
                        format!("is {:?}, not file", metadata.file_type()),
                    );
                    return;
                } else if self.remote_metadata.content_length == 0 {
                    self.state = DownloaderState::Finalize;
                } else if metadata.len() == 0 {
                    self.state = DownloaderState::SuffixDownload(
                        0,
                        <md5::Md5 as md5::Digest>::new(),
                    );
                } else {
                    self.state = DownloaderState::PrefixHash(std::cmp::min(
                        metadata.len(),
                        self.remote_metadata.content_length,
                    ));
                }
                self.local_metadata = Some(metadata);
            }
            Err(e) => {
                self.set_state_to_local_error(ctx, e);
                return;
            }
        }

        self.local_file = Some(file);
        let dev_ino: crate::ctx::DevIno = self.get_dev_ino();
        let (num_in_flight, conflict) = {
            let mut in_flight = ctx.in_flight.lock().expect("download");
            let num_in_flight: usize = in_flight.len();
            match in_flight.entry(dev_ino.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    self.dev_ino = Some(dev_ino);
                    entry.insert((
                        self.remote_path.clone(),
                        self.local_path.clone(),
                    ));
                    (num_in_flight + 1, None)
                }
                std::collections::btree_map::Entry::Occupied(entry) => {
                    (num_in_flight, Some(entry.get().clone()))
                }
            }
        };

        // We have to keep a close eye on how many we have in-flight, because
        // otherwise our file descriptor usage could balloon. We should have
        // no more than:
        //   1 just finishing up being opened (this one!),
        //   1 in the queue to be prefix-hashed,
        //   1 being prefix-hashed,
        //   1 in the queue to be downloaded,
        //   N being downloaded,
        //   1 in the queue to be suffix-hashed,
        //   1 being suffix-hashed.
        //   1 in the queue to be finalized, and
        //   1 being finalized.
        assert!(
            num_in_flight <= ((ctx.base_ctx.data_concurrency as usize) + 8)
        );

        if let Some((conflicting_remote_path, conflicting_local_path)) =
            conflict
        {
            self.set_state_to_local_error(
                ctx,
                format!(
                    "cannot download {} to {:?} because download of {} to {:?} already in progress, and they are the same file",
                    &self.remote_path, &self.local_path, &conflicting_remote_path, &conflicting_local_path
                ),
            );
        }
    }

    pub fn prefix_hash_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, DownloaderState::PrefixHash(_)) {
            panic!(
                "State is {:?}, expected DownloaderState::PrefixHash",
                &self.state
            );
        }

        self.set_state_to_local_error(ctx, err);
    }

    pub fn prefix_hash_succeeded(
        &mut self,
        _ctx: &std::sync::Arc<crate::ctx::Ctx>,
        hasher: md5::Md5,
    ) {
        let DownloaderState::PrefixHash(marker_pos) = self.state else {
            panic!(
                "State is {:?}, expected DownloaderState::PrefixHash",
                &self.state
            );
        };

        if marker_pos == self.remote_metadata.content_length {
            let hash: [u8; 16] = <md5::Md5 as md5::Digest>::finalize(hasher)
                .as_slice()
                .try_into()
                .unwrap();
            if &hash
                == self
                    .remote_metadata
                    .content_md5
                    .as_ref()
                    .unwrap()
                    .as_slice()
            {
                self.state = DownloaderState::Finalize;
            } else {
                self.state = DownloaderState::SuffixDownload(
                    0,
                    <md5::Md5 as md5::Digest>::new(),
                );
            }
        } else {
            assert!(marker_pos < self.remote_metadata.content_length);
            self.state = DownloaderState::SuffixDownload(marker_pos, hasher);
        }
    }

    pub fn download_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(
            self.state,
            DownloaderState::SuffixDownload(_, _)
                | DownloaderState::PrefixDownload(_)
        ) {
            panic!("State is {:?}, expected DownloaderState::SuffixDownload or DownloaderState::PrefixDownload", &self.state);
        }

        self.set_state_to_remote_error(ctx, err);
    }

    pub fn suffix_download_succeeded(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        hash: [u8; 16],
    ) {
        let DownloaderState::SuffixDownload(marker_pos, _) = self.state else {
            panic!(
                "State is {:?}, expected DownloaderState::SuffixDownload",
                &self.state
            );
        };

        if &hash
            == self
                .remote_metadata
                .content_md5
                .as_ref()
                .unwrap()
                .as_slice()
        {
            self.state = DownloaderState::Finalize;
        } else if marker_pos == 0 {
            self.set_state_to_local_error(
                ctx,
                "downloaded file has wrong hash",
            );
        } else {
            self.state = DownloaderState::PrefixDownload(marker_pos);
        }
    }

    pub fn prefix_download_succeeded(
        &mut self,
        _ctx: &std::sync::Arc<crate::ctx::Ctx>,
        hasher: md5::Md5,
    ) {
        let DownloaderState::PrefixDownload(marker_pos) = self.state else {
            panic!(
                "State is {:?}, expected DownloaderState::PrefixDownload",
                &self.state
            );
        };

        // The marker must be strictly less than the size. The only way the
        // prefix download could have been for the whole file would be
        // if they did a prefix hash of the whole file, and then a suffix
        // download of zero bytes. But that would never have happened. If they
        // had done a prefix hash of the whole file, and it was the wrong
        // hash, the kind of download they would have done would have been a
        // suffix download starting from zero, not a prefix download of the
        // whole file.
        assert!(marker_pos < self.remote_metadata.content_length);

        self.state = DownloaderState::SuffixHash(marker_pos, hasher);
    }

    pub fn suffix_hash_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, DownloaderState::SuffixHash(_, _)) {
            panic!(
                "State is {:?}, expected DownloaderState::SuffixHash",
                &self.state
            );
        }

        self.set_state_to_local_error(ctx, err);
    }

    pub fn suffix_hash_succeeded(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        hash: [u8; 16],
    ) {
        let DownloaderState::SuffixHash(_, _) = self.state else {
            panic!(
                "State is {:?}, expected DownloaderState::SuffixHash",
                &self.state
            );
        };

        if &hash
            == self
                .remote_metadata
                .content_md5
                .as_ref()
                .unwrap()
                .as_slice()
        {
            self.state = DownloaderState::Finalize;
        } else {
            self.set_state_to_local_error(
                ctx,
                "downloaded file has wrong hash",
            );
        }
    }

    pub fn finalize_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, DownloaderState::Finalize) {
            panic!(
                "State is {:?}, expected DownloaderState::Finalize",
                &self.state
            );
        }

        self.set_state_to_local_error(ctx, err);
    }

    pub fn finalize_succeeded(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
    ) {
        if !matches!(self.state, DownloaderState::Finalize) {
            panic!(
                "State is {:?}, expected DownloaderState::Finalize",
                &self.state
            );
        }

        if let Some(dev_ino) = self.dev_ino.take() {
            let mut in_flight = ctx.in_flight.lock().expect("download");
            let _ = in_flight.remove(&dev_ino);
        }
        self.local_file = None;
        self.state = DownloaderState::Ok;
    }

    fn set_state_to_local_error<T>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: T,
    ) where
        T: std::fmt::Display,
    {
        if let Some(dev_ino) = self.dev_ino.take() {
            let mut in_flight = ctx.in_flight.lock().expect("download");
            let _ = in_flight.remove(&dev_ino);
        }
        self.local_file = None;
        self.state =
            DownloaderState::Error(format!("{:?}: {}", &self.local_path, err));
    }

    fn set_state_to_remote_error<T>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: T,
    ) where
        T: std::fmt::Display,
    {
        if let Some(dev_ino) = self.dev_ino.take() {
            let mut in_flight = ctx.in_flight.lock().expect("download");
            let _ = in_flight.remove(&dev_ino);
        }
        self.local_file = None;
        self.state =
            DownloaderState::Error(format!("{}: {}", &self.remote_path, err));
    }

    #[cfg(unix)]
    fn get_dev_ino(&self) -> crate::ctx::DevIno {
        let local_metadata: &std::fs::Metadata =
            self.local_metadata.as_ref().unwrap();
        (
            std::os::unix::fs::MetadataExt::dev(local_metadata),
            std::os::unix::fs::MetadataExt::ino(local_metadata),
        )
    }

    #[cfg(not(unix))]
    fn get_dev_ino(&self) -> crate::ctx::DevIno {
        std::fs::canonicalize(self.local_path).expect(&self.local_path)
    }
}

#[test]
fn slash_goes_straight_to_ok() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "/".into(),
        wuffblob::util::fake_blob_properties_directory(),
        true,
    );

    assert!(
        matches!(downloader.state, DownloaderState::Ok),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn dir_goes_to_mkdir() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_blob_properties_directory(),
        true,
    );

    assert!(
        matches!(downloader.state, DownloaderState::Mkdir(_)),
        "State: {:?}",
        &downloader.state
    );
    if let DownloaderState::Mkdir(p) = downloader.state {
        assert_eq!(p, std::path::Path::new("foo/bar"));
    }
}

#[test]
fn file_cannot_be_named_dotdot() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/..".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );

    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn file_goes_to_mkdir_for_parent() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );

    assert!(
        matches!(downloader.state, DownloaderState::Mkdir(_)),
        "State: {:?}",
        &downloader.state
    );
    if let DownloaderState::Mkdir(p) = downloader.state {
        assert_eq!(p, std::path::Path::new("foo"));
    }
}

#[test]
fn file_with_no_parent_goes_straight_to_open() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );

    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_mkdir_goes_to_error() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_blob_properties_directory(),
        true,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Mkdir(_)),
        "State: {:?}",
        &downloader.state
    );

    downloader
        .mkdir_failed(&ctx, wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn mkdir_for_directory_goes_to_ok() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_blob_properties_directory(),
        true,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Mkdir(_)),
        "State: {:?}",
        &downloader.state
    );

    downloader.mkdir_succeeded(&ctx);
    assert!(
        matches!(downloader.state, DownloaderState::Ok),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn mkdir_for_file_goes_to_open() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Mkdir(_)),
        "State: {:?}",
        &downloader.state
    );

    downloader.mkdir_succeeded(&ctx);
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_open_goes_to_error() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );

    downloader.open_failed(&ctx, wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn open_goes_to_finalize_if_blob_is_zero_bytes() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            0,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );

    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn open_with_zero_size_goes_to_suffix_download() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );

    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn open_with_nonzero_size_goes_to_prefix_hash() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );

    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(13)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_prefix_hash_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(13)),
        "State: {:?}",
        &downloader.state
    );

    downloader.prefix_hash_failed(
        &ctx,
        wuffblob::error::WuffError::from("squeeeee"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn prefix_hash_with_incomplete_file_goes_to_suffix_download() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(13)),
        "State: {:?}",
        &downloader.state
    );

    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(13, _)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn prefix_hash_with_complete_file_and_correct_hash_goes_to_finalize() {
    // echo -n 'Hello, world!' | md5
    let correct_hash = [
        0x6cu8, 0xd3u8, 0x55u8, 0x6du8, 0xebu8, 0x0du8, 0xa5u8, 0x4bu8,
        0xcau8, 0x06u8, 0x0bu8, 0x4cu8, 0x39u8, 0x47u8, 0x98u8, 0x39u8,
    ];
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&correct_hash),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(13)),
        "State: {:?}",
        &downloader.state
    );

    let mut hasher = <md5::Md5 as md5::Digest>::new();
    <md5::Md5 as md5::Digest>::update(&mut hasher, "Hello, world!");
    downloader.prefix_hash_succeeded(&ctx, hasher);
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn prefix_hash_with_complete_file_and_incorrect_hash_goes_to_suffix_download()
{
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(
        &ctx,
        wuffblob::util::temp_local_file("Hello, world!"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(13)),
        "State: {:?}",
        &downloader.state
    );

    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_suffix_download_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader
        .download_failed(&ctx, wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn suffix_download_with_correct_hash_goes_to_finalize() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_download_succeeded(&ctx, [23u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn full_suffix_download_with_incorrect_hash_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn partial_suffix_download_with_incorrect_hash_goes_to_prefix_download() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_prefix_download_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );

    downloader
        .download_failed(&ctx, wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn successful_prefix_download_goes_to_suffix_hash() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );

    downloader
        .prefix_download_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixHash(7, _)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_suffix_hash_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .prefix_download_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixHash(7, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_hash_failed(
        &ctx,
        wuffblob::error::WuffError::from("squeeeee"),
    );
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn successful_suffix_hash_with_incorrect_hash_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .prefix_download_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixHash(7, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_hash_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn successful_suffix_hash_with_correct_hash_goes_to_finalize() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            13,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .open_succeeded(&ctx, wuffblob::util::temp_local_file("Hello, "));
    assert!(
        matches!(downloader.state, DownloaderState::PrefixHash(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader.prefix_hash_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(7, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [42u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::PrefixDownload(7)),
        "State: {:?}",
        &downloader.state
    );
    downloader
        .prefix_download_succeeded(&ctx, <md5::Md5 as md5::Digest>::new());
    assert!(
        matches!(downloader.state, DownloaderState::SuffixHash(7, _)),
        "State: {:?}",
        &downloader.state
    );

    downloader.suffix_hash_succeeded(&ctx, [23u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn failed_finalize_goes_to_err() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [23u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );

    downloader
        .finalize_failed(&ctx, wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(downloader.state, DownloaderState::Error(_)),
        "State: {:?}",
        &downloader.state
    );
}

#[test]
fn successful_finalize_goes_to_ok() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut downloader = Downloader::new(
        &ctx,
        "foo/bar".into(),
        "foo".into(),
        wuffblob::util::fake_blob_properties_file(
            "text/plain",
            42,
            Some(&[23u8; 16]),
        ),
        false,
    );
    assert!(
        matches!(downloader.state, DownloaderState::Open),
        "State: {:?}",
        &downloader.state
    );
    downloader.open_succeeded(&ctx, wuffblob::util::temp_local_file(""));
    assert!(
        matches!(downloader.state, DownloaderState::SuffixDownload(0, _)),
        "State: {:?}",
        &downloader.state
    );
    downloader.suffix_download_succeeded(&ctx, [23u8; 16]);
    assert!(
        matches!(downloader.state, DownloaderState::Finalize),
        "State: {:?}",
        &downloader.state
    );

    downloader.finalize_succeeded(&ctx);
    assert!(
        matches!(downloader.state, DownloaderState::Ok),
        "State: {:?}",
        &downloader.state
    );
}
