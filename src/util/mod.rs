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
    helper: std::sync::Arc<BoundedParallelismHelper<Result<T, crate::error::WuffError>>>,
}

impl<T: Send + 'static> BoundedParallelism<T> {
    pub fn new(parallelism: u16) -> Self {
        Self {
            parallelism: parallelism,
            helper: std::sync::Arc::new(BoundedParallelismHelper::<
                Result<T, crate::error::WuffError>,
            > {
                inside_mutex: std::sync::Mutex::new(BoundedParallelismHelperInsideMutex::<
                    Result<T, crate::error::WuffError>,
                > {
                    currently_running: 0u16,
                    results: Vec::new(),
                }),
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
                let mut inside_mutex = self.helper.inside_mutex.lock().expect("BoundedParallelism");
                if inside_mutex.currently_running < self.parallelism {
                    inside_mutex.currently_running += 1;

                    let task: tokio::task::JoinHandle<T> = ctx.get_async_spawner().spawn(f);

                    let fut = {
                        let helper = std::sync::Arc::clone(&self.helper);
                        async move {
                            let result = task.await;
                            let mut inside_for_watcher =
                                helper.inside_mutex.lock().expect("BoundedParallelism");
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
                let mut inside_mutex = self.helper.inside_mutex.lock().expect("BoundedParallelism");
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
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
];
pub fn hex_encode(buf: &[u8]) -> String {
    let mut s: String = String::with_capacity(buf.len() * 2);
    for i in buf {
        s.push(HEX_DIGITS[(i >> 4) as usize]);
        s.push(HEX_DIGITS[(i & 0xfu8) as usize]);
    }
    s
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
