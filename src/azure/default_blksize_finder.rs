// This module wouldn't have to exist if
// azure_core::SeekableStream::DEFAULT_BUFFER_SIZE weren't private.
//
// The sum total of what this module does, is to retrieve the value of
// azure_core::SeekableStream::DEFAULT_BUFFER_SIZE despite its being private.

#[derive(Debug, Clone)]
struct FakeThing {}

impl azure_core::SeekableStream for FakeThing {
    fn reset<'a, 'fut>(
        &'a mut self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<(), azure_core::error::Error>,
                > + Send
                + 'fut,
        >,
    >
    where
        Self: 'fut,
        'a: 'fut,
    {
        Box::pin(async { Ok(()) })
    }

    fn len(&self) -> usize {
        0
    }
}

impl futures::io::AsyncRead for FakeThing {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        mut buf: &mut [u8],
    ) -> std::task::Poll<Result<usize, futures::io::Error>> {
        std::task::Poll::Ready(Ok(0))
    }
}

pub fn get_azure_seekable_stream_default_buffer_size() -> usize {
    azure_core::SeekableStream::buffer_size(&FakeThing {})
}

#[test]
fn test_get_default_buffer_size() {
    let buffer_size = get_azure_seekable_stream_default_buffer_size();
    assert!(buffer_size > 0);
}
