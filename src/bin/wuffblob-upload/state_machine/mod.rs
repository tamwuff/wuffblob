#[derive(Debug, Clone)]
pub enum UploaderState {
    // Success! I don't need to be put into any queue.
    Ok,

    // Please put the string contained herein, into the error queue. This
    // is a terminal state as far as the state machine is concerned. I,
    // myself, am not being enqueued anywhere. Only my error message is.
    Error(String),

    // Please put me into the metadata queue.
    GetRemoteMetadata,

    // Please put me into the metadata queue.
    Mkdir,

    // Please put me into the queue to be hashed.
    Hash,

    // Please put me into the queue to be uploaded. When I get there, I
    // will/won't need to be deleted first.
    Upload(bool),

    // Please put me into the queue to be verified. I am a file. Directories
    // don't get here.
    Verify,
}

#[derive(Debug)]
pub struct Uploader {
    pub local_path: std::path::PathBuf,
    pub remote_path: wuffblob::path::WuffPath,
    pub state: UploaderState,
    pub local_metadata: std::fs::Metadata,
    pub remote_metadata: Option<azure_storage_blobs::blob::BlobProperties>,
    pub desired_content_type: &'static str,
    pub local_md5: Option<[u8; 16]>,
}

impl Uploader {
    fn new(
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        local_path: std::path::PathBuf,
        remote_path: wuffblob::path::WuffPath,
        local_metadata: std::fs::Metadata,
    ) -> Uploader {
        // They were only supposed to give us canonical paths that were
        // either files or directories. Just double check...
        assert!(remote_path.is_canonical());
        let file_type: std::fs::FileType = local_metadata.file_type();
        assert!(file_type.is_file() || file_type.is_dir());

        let desired_content_type: &'static str = if file_type.is_file() {
            ctx.base_ctx.get_desired_mime_type(&remote_path)
        } else {
            ""
        };

        Uploader {
            local_path: local_path,
            remote_path: remote_path,
            state: UploaderState::GetRemoteMetadata,
            local_metadata: local_metadata,
            remote_metadata: None,
            desired_content_type: desired_content_type,
            local_md5: None,
        }
    }

    pub fn get_remote_metadata_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, UploaderState::GetRemoteMetadata) {
            panic!(
                "State is {:?}, expected UploaderState::GetRemoteMetadata",
                &self.state
            );
        }

        self.set_state_to_remote_error(err);
    }

    pub fn there_is_no_remote_metadata(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if !matches!(self.state, UploaderState::GetRemoteMetadata) {
            panic!(
                "State is {:?}, expected UploaderState::GetRemoteMetadata",
                &self.state
            );
        }

        if self.local_metadata.file_type().is_file() {
            self.state = UploaderState::Upload(false);
        } else {
            self.state = UploaderState::Mkdir;
        }
    }

    pub fn provide_remote_metadata(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        remote_metadata: azure_storage_blobs::blob::BlobProperties,
    ) {
        if !matches!(self.state, UploaderState::GetRemoteMetadata) {
            panic!(
                "State is {:?}, expected UploaderState::GetRemoteMetadata",
                &self.state
            );
        }
        self.remote_metadata = Some(remote_metadata);
        let remote_metadata: &azure_storage_blobs::blob::BlobProperties =
            self.remote_metadata.as_ref().unwrap();

        let mut remote_is_dir: bool = false;
        if let Some(ref resource_type) = remote_metadata.resource_type {
            if resource_type == "directory" {
                remote_is_dir = true;
            }
        }

        if self.local_metadata.file_type().is_file() {
            if remote_is_dir {
                self.set_state_to_remote_error("is directory, not file");
            } else {
                if remote_metadata.content_md5.is_none() {
                    self.set_state_to_remote_error("no hash, please run wuffblob-fsck");
                } else if remote_metadata.content_length == self.local_metadata.len() {
                    self.state = UploaderState::Hash;
                } else if ctx.force {
                    self.state = UploaderState::Upload(true);
                } else {
                    self.set_state_to_remote_error(format!(
                        "is {} bytes, should be {}",
                        remote_metadata.content_length,
                        self.local_metadata.len()
                    ));
                }
            }
        } else {
            if remote_is_dir {
                self.state = UploaderState::Ok;
            } else {
                self.set_state_to_remote_error("is file, not directory");
            }
        }
    }

    pub fn hash_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, UploaderState::Hash) {
            panic!("State is {:?}, expected UploaderState::Hash", &self.state);
        }

        self.set_state_to_local_error(err);
    }

    pub fn provide_hash(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>, empirical_md5: [u8; 16]) {
        if !matches!(self.state, UploaderState::Hash) {
            panic!("State is {:?}, expected UploaderState::Hash", &self.state);
        }
        let remote_metadata: &azure_storage_blobs::blob::BlobProperties =
            self.remote_metadata.as_ref().unwrap();

        self.local_md5 = Some(empirical_md5);
        if self.local_md5.as_ref().unwrap()
            == remote_metadata.content_md5.as_ref().unwrap().as_slice()
        {
            self.state = UploaderState::Ok;
        } else if ctx.force {
            self.state = UploaderState::Upload(true);
        } else {
            self.set_state_to_remote_error(format!(
                "is {} bytes, should be {}",
                remote_metadata.content_length,
                self.local_metadata.len()
            ));
        }
    }

    pub fn mkdir_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, UploaderState::Mkdir) {
            panic!("State is {:?}, expected UploaderState::Mkdir", &self.state);
        }

        self.set_state_to_remote_error(err);
    }

    pub fn mkdir_succeeded(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if !matches!(self.state, UploaderState::Mkdir) {
            panic!("State is {:?}, expected UploaderState::Mkdir", &self.state);
        }

        self.state = UploaderState::Ok;
    }

    pub fn upload_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, UploaderState::Upload(_)) {
            panic!("State is {:?}, expected UploaderState::Upload", &self.state);
        }

        self.set_state_to_remote_error(err);
    }

    pub fn upload_succeeded(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if !matches!(self.state, UploaderState::Upload(_)) {
            panic!("State is {:?}, expected UploaderState::Upload", &self.state);
        }

        self.state = if ctx.verify {
            UploaderState::Verify
        } else {
            UploaderState::Ok
        };
    }

    pub fn verify_failed(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        err: &wuffblob::error::WuffError,
    ) {
        if !matches!(self.state, UploaderState::Verify) {
            panic!("State is {:?}, expected UploaderState::Verify", &self.state);
        }

        self.set_state_to_remote_error(err);
    }

    pub fn verify_succeeded(&mut self, ctx: &std::sync::Arc<crate::ctx::Ctx>) {
        if !matches!(self.state, UploaderState::Verify) {
            panic!("State is {:?}, expected UploaderState::Verify", &self.state);
        }

        self.state = UploaderState::Ok;
    }

    fn set_state_to_local_error<T>(&mut self, err: T)
    where
        T: std::fmt::Display,
    {
        self.state = UploaderState::Error(format!("{:?}: {}", &self.local_path, err));
    }

    fn set_state_to_remote_error<T>(&mut self, err: T)
    where
        T: std::fmt::Display,
    {
        self.state = UploaderState::Error(format!("{}: {}", &self.remote_path, err));
    }
}

#[test]
fn dir_goes_straight_to_get_metadata_with_no_content_type() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    assert_eq!(uploader.desired_content_type, "");
}

#[test]
fn file_goes_straight_to_get_metadata_with_proper_content_type() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    assert_eq!(uploader.desired_content_type, "text/plain");
}

#[test]
fn dir_goes_to_error_if_get_metadata_fails() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.get_remote_metadata_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_get_metadata_fails() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.get_remote_metadata_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn dir_goes_to_mkdir_if_remote_metadata_nonexistent() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Mkdir),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_upload_if_remote_metadata_nonexistent() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn dir_goes_to_error_if_remote_metadata_indicates_file() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_remote_metadata_indicates_dir() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(&ctx, wuffblob::util::fake_blob_properties_directory());
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn dir_goes_to_ok_if_already_exists() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(&ctx, wuffblob::util::fake_blob_properties_directory());
    assert!(
        matches!(uploader.state, UploaderState::Ok),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_remote_metadata_indicates_wrong_size_and_not_force() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().force = false;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_upload_if_remote_metadata_indicates_wrong_size_and_force() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().force = true;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 42, Some(&[23u8; 16])),
    );
    assert!(
        matches!(uploader.state, UploaderState::Upload(true)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_hash_if_remote_metadata_indicates_correct_size() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 23, Some(&[23u8; 16])),
    );
    assert!(
        matches!(uploader.state, UploaderState::Hash),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_local_hashing_fails() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 23, Some(&[23u8; 16])),
    );
    assert!(
        matches!(uploader.state, UploaderState::Hash),
        "State: {:?}",
        &uploader.state
    );

    uploader.hash_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_hash_is_wrong_and_not_force() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().force = false;
    let incorrect_hash = [23u8; 16];
    let correct_hash = [42u8; 16];
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 23, Some(&incorrect_hash)),
    );
    assert!(
        matches!(uploader.state, UploaderState::Hash),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_hash(&ctx, correct_hash);
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_upload_if_hash_is_wrong_and_force() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().force = true;
    let incorrect_hash = [23u8; 16];
    let correct_hash = [42u8; 16];
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.provide_remote_metadata(
        &ctx,
        wuffblob::util::fake_blob_properties_file("text/plain", 23, Some(&incorrect_hash)),
    );
    assert!(
        matches!(uploader.state, UploaderState::Hash),
        "State: {:?}",
        &uploader.state
    );

    uploader.provide_hash(&ctx, correct_hash);
    assert!(
        matches!(uploader.state, UploaderState::Upload(true)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn dir_goes_to_error_if_mkdir_fails() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Mkdir),
        "State: {:?}",
        &uploader.state
    );

    uploader.mkdir_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn dir_goes_to_ok_if_mkdir_succeeds() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar".into(),
        "foo/bar".into(),
        wuffblob::util::fake_local_metadata(None),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Mkdir),
        "State: {:?}",
        &uploader.state
    );

    uploader.mkdir_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Ok),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_upload_fails() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );

    uploader.upload_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_ok_if_upload_succeeds_and_not_verify() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().verify = false;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );

    uploader.upload_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Ok),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_verify_if_upload_succeeds_and_verify() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().verify = true;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );

    uploader.upload_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Verify),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_error_if_verify_fails() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().verify = true;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );
    uploader.upload_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Verify),
        "State: {:?}",
        &uploader.state
    );

    uploader.verify_failed(&ctx, &wuffblob::error::WuffError::from("squeeeee"));
    assert!(
        matches!(uploader.state, UploaderState::Error(_)),
        "State: {:?}",
        &uploader.state
    );
}

#[test]
fn file_goes_to_ok_if_verify_succeeds() {
    let mut ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal("foo", "foo"));
    std::sync::Arc::get_mut(&mut ctx).unwrap().verify = true;
    let mut uploader = Uploader::new(
        &ctx,
        "foo/bar.txt".into(),
        "foo/bar.txt".into(),
        wuffblob::util::fake_local_metadata(Some(23)),
    );
    assert!(
        matches!(uploader.state, UploaderState::GetRemoteMetadata),
        "State: {:?}",
        &uploader.state
    );
    uploader.there_is_no_remote_metadata(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Upload(false)),
        "State: {:?}",
        &uploader.state
    );
    uploader.upload_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Verify),
        "State: {:?}",
        &uploader.state
    );

    uploader.verify_succeeded(&ctx);
    assert!(
        matches!(uploader.state, UploaderState::Ok),
        "State: {:?}",
        &uploader.state
    );
}
