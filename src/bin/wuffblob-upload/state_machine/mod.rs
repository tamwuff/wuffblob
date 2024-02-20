#[derive(Debug, Clone)]
pub enum UploaderState {
    // Success! I don't need to be put into any queue.
    Ok,

    // Please put me into the error queue.
    Err(String),

    // Please put me into the metadata queue.
    NeedGet,

    // Please put me into the metadata queue.
    NeedMkdir,

    // Please put me into the queue to be hashed.
    Hash,

    // Please put me into the queue to be uploaded.
    Upload,
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

        Uploader {
            local_path: local_path,
            remote_path: remote_path,
            state: UploaderState::NeedGet,
            local_metadata: local_metadata,
            remote_metadata: None,
            desired_content_type: "",
            local_md5: None,
        }
    }
}
