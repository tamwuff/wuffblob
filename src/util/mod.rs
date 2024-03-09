// Bounded parallelism: keep track of how many (foo's) are running, don't let
// another one start until one of the existing ones has finished.

struct BoundedParallelismHelperInsideMutex<T> {
    currently_running: u16,
    results: Vec<T>,
}

struct BoundedParallelismHelper<T> {
    inside_mutex: std::sync::Mutex<BoundedParallelismHelperInsideMutex<T>>,

    // tokio doesn't have real condition variables. this is as close as it gets
    cv: tokio::sync::Notify,
}

pub struct BoundedParallelism<T> {
    parallelism: u16,
    helper: std::sync::Arc<
        BoundedParallelismHelper<Result<T, crate::error::WuffError>>,
    >,
}

impl<T: Send + 'static> BoundedParallelism<T> {
    pub fn new(parallelism: u16) -> Self {
        Self {
            parallelism: parallelism,
            helper: std::sync::Arc::new(BoundedParallelismHelper::<
                Result<T, crate::error::WuffError>,
            > {
                inside_mutex: std::sync::Mutex::new(
                    BoundedParallelismHelperInsideMutex::<
                        Result<T, crate::error::WuffError>,
                    > {
                        currently_running: 0u16,
                        results: Vec::new(),
                    },
                ),
                cv: tokio::sync::Notify::new(),
            }),
        }
    }

    pub async fn spawn<F>(
        &self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        f: F,
    ) -> Vec<Result<T, crate::error::WuffError>>
    where
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let mut results: Vec<Result<T, crate::error::WuffError>> = Vec::new();
        loop {
            {
                let mut inside_mutex = self
                    .helper
                    .inside_mutex
                    .lock()
                    .expect("BoundedParallelism");
                if inside_mutex.currently_running < self.parallelism {
                    inside_mutex.currently_running += 1;

                    let task: tokio::task::JoinHandle<T> =
                        ctx.get_async_spawner().spawn(f);

                    let fut = {
                        let helper = std::sync::Arc::clone(&self.helper);
                        async move {
                            let result = task.await;
                            let mut inside_for_watcher = helper
                                .inside_mutex
                                .lock()
                                .expect("BoundedParallelism");
                            inside_for_watcher.currently_running -= 1;
                            inside_for_watcher.results.push(match result {
                                Ok(x) => Ok(x),
                                Err(e) => Err(e.into()),
                            });
                            helper.cv.notify_one();
                        }
                    };
                    let _ = ctx.get_async_spawner().spawn(fut);

                    std::mem::swap(&mut results, &mut inside_mutex.results);
                    break;
                }
            }
            self.helper.cv.notified().await
        }
        results
    }

    pub async fn drain(&self) -> Vec<Result<T, crate::error::WuffError>> {
        let mut results: Vec<Result<T, crate::error::WuffError>> = Vec::new();
        loop {
            {
                let mut inside_mutex = self
                    .helper
                    .inside_mutex
                    .lock()
                    .expect("BoundedParallelism");
                if inside_mutex.currently_running == 0u16 {
                    std::mem::swap(&mut results, &mut inside_mutex.results);
                    break;
                }
            }
            self.helper.cv.notified().await
        }
        results
    }
}

// Hex stuff... is there really nothing built in for this??

static HEX_DIGITS: [char; 16] = [
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e',
    'f',
];
pub fn hex_encode(buf: &[u8]) -> String {
    let mut s: String = String::with_capacity(buf.len() * 2);
    for i in buf {
        s.push(HEX_DIGITS[(i >> 4) as usize]);
        s.push(HEX_DIGITS[(i & 0xfu8) as usize]);
    }
    s
}

// For unit testing we want to be able to make Metadata objects or File objects
// to our specifications. There is no public constructor for Metadata, so the
// only way to get one is to actually create real temporary files/directories.
// Which is the same thing we need to do to make a File object for unit
// testing, too.
fn make_temp_file_or_dir(
    contents: Option<&[u8]>,
) -> (std::fs::Metadata, Option<std::fs::File>) {
    let mut rng: rand::rngs::ThreadRng = rand::thread_rng();
    let mut uuid_buf: [u8; 16] = [0u8; 16];
    rand::RngCore::fill_bytes(&mut rng, &mut uuid_buf);
    let uuid_str: String = hex_encode(&uuid_buf);

    let metadata: std::fs::Metadata;
    let mut f: Option<std::fs::File> = None;

    let mut dir_name: std::path::PathBuf = std::env::temp_dir();
    dir_name.push(&uuid_str);
    let dir_name_for_panic: String = format!("{:?}", &dir_name);
    std::fs::create_dir(&dir_name).expect(&dir_name_for_panic);

    if let Some(buf) = contents {
        let mut file_name: std::path::PathBuf = dir_name.clone();
        file_name.push(&uuid_str);
        let file_name_for_panic: String = format!("{:?}", &file_name);
        std::io::Write::write_all(
            &mut (std::fs::File::create(&file_name)
                .expect(&file_name_for_panic)),
            buf,
        )
        .expect(&file_name_for_panic);

        metadata = std::fs::metadata(&file_name).expect(&file_name_for_panic);
        f = Some(std::fs::File::open(&file_name).expect(&file_name_for_panic));

        std::fs::remove_file(&file_name).expect(&file_name_for_panic);
    } else {
        metadata = std::fs::metadata(&dir_name).expect(&dir_name_for_panic);
    }

    std::fs::remove_dir(&dir_name).expect(&dir_name_for_panic);

    (metadata, f)
}

#[allow(dead_code)]
pub fn fake_local_metadata(desired_size: Option<u64>) -> std::fs::Metadata {
    let (metadata, f) = if let Some(nbytes) = desired_size {
        let mut v: Vec<u8> = Vec::with_capacity(nbytes as usize);
        v.resize(nbytes as usize, 0);
        make_temp_file_or_dir(Some(v.as_slice()))
    } else {
        make_temp_file_or_dir(None)
    };
    metadata
}

#[allow(dead_code)]
pub fn temp_local_file(contents: &str) -> std::fs::File {
    let (metadata, f) = make_temp_file_or_dir(Some(contents.as_bytes()));
    f.unwrap()
}

// For unit testing we want to be able to make BlobProperties objects to our
// specifications. This is really painful, since there is no public
// constructor. The only way I can find to do it is to deserialize a minimal
// one from JSON, which can then be customized to our needs.
//
// The fields given here were reverse engineered by starting with "{}" and
// every time it barfed because of a missing field, I added only that one
// field.
fn minimal_fake_blob_properties() -> azure_storage_blobs::blob::BlobProperties
{
    let s: &str = "{\"Creation-Time\": \"Mon, 01 Jan 1970 00:00:00 GMT\", \"Last-Modified\": \"Mon, 01 Jan 1970 00:00:00 GMT\", \"Etag\": \"\", \"Content-Length\": 0, \"Content-Type\": \"\", \"BlobType\": \"BlockBlob\"}";
    serde_json::from_str::<azure_storage_blobs::blob::BlobProperties>(s)
        .expect(s)
}

// for unit tests
#[allow(dead_code)]
pub fn fake_blob_properties_file(
    content_type: &str,
    content_length: u64,
    hash: Option<&[u8; 16]>,
) -> azure_storage_blobs::blob::BlobProperties {
    let mut blob_properties: azure_storage_blobs::blob::BlobProperties =
        minimal_fake_blob_properties();
    blob_properties.content_type = content_type.to_string();
    blob_properties.resource_type = Some("file".to_string());
    blob_properties.content_length = content_length;
    if let Some(buf) = hash {
        blob_properties.content_md5 = Some(
            azure_storage::prelude::ConsistencyMD5::decode(
                base64::Engine::encode(&base64::prelude::BASE64_STANDARD, buf),
            )
            .unwrap(),
        );
    }
    blob_properties
}

// for unit tests
#[allow(dead_code)]
pub fn fake_blob_properties_directory(
) -> azure_storage_blobs::blob::BlobProperties {
    let mut blob_properties: azure_storage_blobs::blob::BlobProperties =
        minimal_fake_blob_properties();
    blob_properties.resource_type = Some("directory".to_string());
    blob_properties
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

#[test]
fn make_fake_local_metadata_for_file() {
    let metadata = fake_local_metadata(Some(23));
    assert!(metadata.file_type().is_file());
    assert!(metadata.len() == 23);
}

#[test]
fn make_fake_local_metadata_for_directory() {
    let metadata = fake_local_metadata(None);
    assert!(metadata.file_type().is_dir());
}

#[test]
fn make_temp_local_file() {
    let mut buf: [u8; 13] = [0u8; 13];
    let mut f = temp_local_file("Hello, world!");

    assert_eq!(std::io::Seek::stream_position(&mut f).unwrap(), 0);
    std::io::Read::read_exact(&mut f, &mut buf)
        .expect("wrong number of bytes");

    assert_eq!(std::io::Seek::stream_position(&mut f).unwrap(), 13);
    assert_eq!(
        std::io::Read::read(&mut f, &mut buf).expect("unexpected error"),
        0
    );
}

#[test]
fn make_fake_blob_properties_for_file_no_hash() {
    let p = fake_blob_properties_file("text/plain", 42, None);
    assert!(p.resource_type.is_some());
    assert_eq!(p.resource_type.unwrap(), "file");
    assert_eq!(p.content_type, "text/plain");
    assert_eq!(p.content_length, 42);
    assert!(p.content_md5.is_none());
}

#[test]
fn make_fake_blob_properties_for_file_with_hash() {
    let v: [u8; 16] = [23u8; 16];
    let p = fake_blob_properties_file("application/pdf", 420, Some(&v));
    assert!(p.resource_type.is_some());
    assert_eq!(p.resource_type.unwrap(), "file");
    assert_eq!(p.content_type, "application/pdf");
    assert_eq!(p.content_length, 420);
    assert!(p.content_md5.is_some());
    assert_eq!(p.content_md5.unwrap().as_slice(), &v);
}

#[test]
fn make_fake_blob_properties_for_directory() {
    let p = fake_blob_properties_directory();
    assert!(p.resource_type.is_some());
    assert_eq!(p.resource_type.unwrap(), "directory");
    assert_eq!(p.content_type, "");
    assert_eq!(p.content_length, 0);
}
