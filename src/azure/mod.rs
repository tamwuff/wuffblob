#[derive(Debug)]
pub struct AzureClient {
    pub container_client: azure_storage_blobs::prelude::ContainerClient,
    pub data_lake_client: azure_storage_datalake::clients::FileSystemClient,
}

impl AzureClient {
    pub fn new(storage_account: &str, access_key: &str, container: &str) -> AzureClient {
        AzureClient {
            container_client: azure_storage_blobs::prelude::ClientBuilder::new(
                storage_account,
                azure_storage::StorageCredentials::access_key(
                    storage_account,
                    access_key.to_string(),
                ),
            )
            .container_client(container),
            data_lake_client: azure_storage_datalake::clients::DataLakeClientBuilder::new(
                storage_account,
                azure_storage::StorageCredentials::access_key(
                    storage_account,
                    access_key.to_string(),
                ),
            )
            .build()
            .file_system_client(container),
        }
    }
}

#[derive(Debug)]
struct FileForUploadInsideMutex {
    f: std::fs::File,
    off: u64,
    bph: crate::bph::BulletproofHasher,
}

impl FileForUploadInsideMutex {
    fn seek(&mut self, off: u64) -> std::io::Result<()> {
        self.off = std::io::Seek::seek(&mut self.f, std::io::SeekFrom::Start(off))?;
        debug_assert!(self.off == off);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct FileChunkForUpload {
    inside_mutex: std::sync::Arc<std::sync::Mutex<FileForUploadInsideMutex>>,
    off_start: u64,
    off_end: u64,

    // The blksize is shared, so it could be inside the mutex with all the
    // other shared data, but it is also constant, so it doesn't need to be
    // inside the mutex.
    #[cfg(unix)]
    blksize: usize,
}

// This is the actual thing that the Azure library wants us to implement.
// We will have to implement more, after this, because SeekableStream itself
// has dependencies. But this is the actual thing we're *trying* to implement,
// here.
impl azure_core::SeekableStream for FileChunkForUpload {
    fn reset<'a, 'fut>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<(), azure_core::error::Error>> + Send + 'fut>,
    >
    where
        Self: 'fut,
        'a: 'fut,
    {
        Box::pin(async {
            let mut inside_mutex = self.inside_mutex.lock().expect("FileChunkForUpload");
            inside_mutex.seek(self.off_start)?;
            Ok(())
        })
    }

    fn len(&self) -> usize {
        (self.off_end - self.off_start) as usize
    }

    // For unix, we know better than the azure library does. For non-unix, the
    // azure library probably has a better default than we could come up with.
    #[cfg(unix)]
    fn buffer_size(&self) -> usize {
        self.blksize
    }
}

// This is only needed because azure_core::SeekableStream needs it as a
// dependency
impl futures::io::AsyncRead for FileChunkForUpload {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, futures::io::Error>> {
        let mut inside_mutex = self.inside_mutex.lock().expect("FileChunkForUpload");
        debug_assert!(
            std::io::Seek::stream_position(&mut inside_mutex.f).expect("lseek()")
                == inside_mutex.off
        );

        let off: u64 = inside_mutex.off;
        debug_assert!(off >= self.off_start);
        debug_assert!(off <= self.off_end);
        if (buf.len() as u64) > (self.off_end - off) {
            buf = &mut buf[..((self.off_end - off) as usize)];
        }

        if buf.is_empty() {
            return std::task::Poll::Ready(Ok(buf.len()));
        }

        if let Err(e) = std::io::Read::read_exact(&mut inside_mutex.f, buf) {
            // we have no idea what the file's actual offset is now, so reset that
            inside_mutex.off =
                std::io::Seek::stream_position(&mut inside_mutex.f).expect("lseek()");
            return std::task::Poll::Ready(Err(e.into()));
        }
        inside_mutex.off += buf.len() as u64;

        inside_mutex.bph.update(off, buf);

        debug_assert!(
            std::io::Seek::stream_position(&mut inside_mutex.f).expect("lseek()")
                == inside_mutex.off
        );
        std::task::Poll::Ready(Ok(buf.len()))
    }
}

pub struct FileForUpload {
    inside_mutex: std::sync::Arc<std::sync::Mutex<FileForUploadInsideMutex>>,
    len: u64,

    // What size we should make the blocks that make up our block blob
    blob_block_size: u64,

    // This is analogous (but not equivalent) to st_blksize in struct stat. It
    // is not about the size of the blocks that are part of the blob, it is
    // about what we recommend to the azure library as far as how we would like
    // to be read.
    #[cfg(unix)]
    blksize: usize,
}

impl FileForUpload {
    pub fn new(mut f: std::fs::File, metadata: &std::fs::Metadata) -> Self {
        let off: u64 = std::io::Seek::stream_position(&mut f).expect("lseek()");

        FileForUpload {
            inside_mutex: std::sync::Arc::new(std::sync::Mutex::new(FileForUploadInsideMutex {
                f: f,
                off: off,
                bph: crate::bph::BulletproofHasher::new(),
            })),
            len: metadata.len(),
            blob_block_size: Self::calc_blob_block_size(metadata.len()),

            #[cfg(unix)]
            blksize: Self::calc_blksize(std::os::unix::fs::MetadataExt::blksize(metadata) as usize),
        }
    }

    pub fn num_blob_blocks(&self) -> u16 {
        assert!(self.blob_block_size > 0);
        let nblocks: u64 = (self.len + self.blob_block_size - 1) / self.blob_block_size;
        assert!(nblocks < 65536);
        nblocks as u16
    }

    pub fn get_chunk_for_upload(&self, block_num: u16) -> FileChunkForUpload {
        let off_start: u64 = (block_num as u64) * self.blob_block_size;
        let mut off_end: u64 = off_start + self.blob_block_size;
        if off_end > self.len {
            off_end = self.len;
        }

        {
            let mut inside_mutex = self.inside_mutex.lock().expect("FileForUpload");
            inside_mutex.seek(off_start).expect("lseek()");
        }

        FileChunkForUpload {
            inside_mutex: std::sync::Arc::clone(&self.inside_mutex),
            off_start: off_start,
            off_end: off_end,

            #[cfg(unix)]
            blksize: self.blksize,
        }
    }

    pub fn get_hash(&self) -> Result<[u8; 16], crate::error::WuffError> {
        let mut inside_mutex = self.inside_mutex.lock().expect("FileForUpload");
        inside_mutex.bph.digest(self.len)
    }

    // this is not public, this is only for unit tests in this module
    fn set_blob_block_size(&mut self, blob_block_size: u64) {
        self.blob_block_size = blob_block_size;
    }

    fn calc_blob_block_size(len: u64) -> u64 {
        // https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
        // A block blob can include a maximum of 50,000 committed blocks
        // Each of which can be up to 4,000 MB
        assert!(len <= (50000 * 4000 * 1048576));
        match len {
            //    1 MB blocks up to     16 *    1 MB
            ..=16777216 => 1048576,

            //    2 MB blocks up to     32 *    2 MB
            ..=67108864 => 2097152,

            //    4 MB blocks up to     64 *    4 MB
            ..=268435456 => 4194304,

            //    8 MB blocks up to    128 *    8 MB = 1 GB
            ..=1073741824 => 8388608,

            //   16 MB blocks up to    256 *   16 MB
            ..=4294967296 => 16777216,

            //   32 MB blocks up to    512 *   32 MB
            ..=17179869184 => 33554432,

            //   64 MB blocks up to  1,024 *   64 MB
            ..=68719476736 => 67108864,

            //  128 MB blocks up to  2,048 *  128 MB
            ..=274877906944 => 134217728,

            //  256 MB blocks up to  4,096 *  256 MB = 1 TB
            ..=1099511627776 => 268435456,

            //  512 MB blocks up to  8,192 *  512 MB
            ..=4398046511104 => 536870912,

            // 1024 MB blocks up to 16,384 * 1024 MB
            ..=17592186044416 => 1073741824,

            // 2048 MB blocks up to 32,768 * 2048 MB = 64 TB
            ..=70368744177664 => 2147483648,

            // 4000 MB blocks up to 50,000 * 4000 MB
            _ => 4194304000,
        }
    }

    #[cfg(unix)]
    fn calc_blksize(mut blksize: usize) -> usize {
        // azure_core::SeekableStream::DEFAULT_BUFFER_SIZE is private, but at
        // the time of this writing, it was 65536
        const DEFAULT_BUFFER_SIZE: usize = 65536;

        assert!(blksize > 0);
        while blksize < DEFAULT_BUFFER_SIZE {
            blksize *= 2;
        }
        blksize
    }
}

#[test]
fn blob_block_size_1gb() {
    const MB: u64 = 1048576;
    const GB: u64 = MB * 1024;
    assert_eq!(FileForUpload::calc_blob_block_size(GB - 1), 8 * MB);
    assert_eq!(FileForUpload::calc_blob_block_size(GB), 8 * MB);
    assert_eq!(FileForUpload::calc_blob_block_size(GB + 1), 16 * MB);
}

#[test]
fn blob_block_size_1tb() {
    const MB: u64 = 1048576;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    assert_eq!(FileForUpload::calc_blob_block_size(TB - 1), 256 * MB);
    assert_eq!(FileForUpload::calc_blob_block_size(TB), 256 * MB);
    assert_eq!(FileForUpload::calc_blob_block_size(TB + 1), 512 * MB);
}

#[test]
fn blob_block_size_64tb() {
    const MB: u64 = 1048576;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;
    assert_eq!(
        FileForUpload::calc_blob_block_size((64 * TB) - 1),
        2048 * MB
    );
    assert_eq!(FileForUpload::calc_blob_block_size(64 * TB), 2048 * MB);
    assert_eq!(
        FileForUpload::calc_blob_block_size((64 * TB) + 1),
        4000 * MB
    );
}

#[test]
fn read_three_chunks_in_all_kinds_of_wild_and_crazy_ways() {
    // echo -n 'Hello, world!' | md5
    let correct_hash = [
        0x6cu8, 0xd3u8, 0x55u8, 0x6du8, 0xebu8, 0x0du8, 0xa5u8, 0x4bu8, 0xcau8, 0x06u8, 0x0bu8,
        0x4cu8, 0x39u8, 0x47u8, 0x98u8, 0x39u8,
    ];

    // We have a file that is 13 bytes long, in total.
    //
    // That will be divided into 3 chunks of 5 bytes each, with the last chunk
    // being short (only 3 bytes).
    //
    // We will read these 5 byte chunks, 3 bytes at a time.

    let mut f = {
        let f = crate::util::temp_local_file("Hello, world!");
        let metadata = f.metadata().unwrap();
        FileForUpload::new(f, &metadata)
    };
    f.set_blob_block_size(5);

    assert_eq!(f.num_blob_blocks(), 3);

    crate::ctx::Ctx::new_minimal()
        .run_async_main(async {
            let mut buf: [u8; 3] = [0u8; 3];

            /////////////////////
            // Chunk The First //
            /////////////////////

            let mut chunk = f.get_chunk_for_upload(0);
            assert_eq!(azure_core::SeekableStream::len(&chunk), 5);

            // We read the first three bytes but then we derp
            {
                let result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(&buf[..3], "Hel".as_bytes());
            }

            // Derp, reset, chickens, turnips, Tallahassee, oh gosh heck I am
            // so confused
            {
                let result = azure_core::SeekableStream::reset(&mut chunk).await;
                assert!(result.is_ok());
            }

            // Now we read all five bytes. And just to be sure, we try
            // reading even more, and make sure we get an EOF.
            {
                let mut result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(&buf[..3], "Hel".as_bytes());

                result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 2);
                assert_eq!(&buf[..2], "lo".as_bytes());

                result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 0);
            }

            //////////////////////
            // Chunk The Second //
            //////////////////////

            let mut chunk = f.get_chunk_for_upload(1);
            assert_eq!(azure_core::SeekableStream::len(&chunk), 5);

            // We read the first three bytes but then we derp
            {
                let result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(&buf[..3], ", w".as_bytes());
            }

            // um why are there so many eyeballs in my salad I think I need a
            // quick nap
            {
                let result = azure_core::SeekableStream::reset(&mut chunk).await;
                assert!(result.is_ok());
            }

            // Now we read all five bytes. And just to be sure, we try
            // reading even more, and make sure we get an EOF.
            {
                let mut result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(&buf[..3], ", w".as_bytes());

                result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 2);
                assert_eq!(&buf[..2], "or".as_bytes());

                result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 0);
            }

            /////////////////////
            // Brief Interlude //
            /////////////////////

            // We shouldn't be able to retrieve the hash yet. If we can,
            // that's a bug.
            let mut hash = f.get_hash();
            assert!(hash.is_err());

            /////////////////////
            // Chunk The Third //
            /////////////////////

            let mut chunk = f.get_chunk_for_upload(2);
            assert_eq!(azure_core::SeekableStream::len(&chunk), 3);

            // Read all three bytes. And just to be sure, we try reading even
            // more, and make sure we get an EOF.
            {
                let mut result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 3);
                assert_eq!(&buf[..3], "ld!".as_bytes());

                result = futures::io::AsyncReadExt::read(&mut chunk, &mut buf).await;
                assert!(result.is_ok());
                assert_eq!(result.unwrap(), 0);
            }

            // Now we should be able to get the hash.
            hash = f.get_hash();
            assert!(hash.is_ok());
            assert_eq!(hash.unwrap(), correct_hash);

            Ok(())
        })
        .expect("unexpected error");
}
