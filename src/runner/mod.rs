// Philosophy:
//
// Two kinds of callbacks you can install for generating Box<T>'s:
//   * blocking (will be spawned in a thread)
//   * async (will be spawned as a task)
// Either one can either return an error result, or panic. Both are treated
// as a fatal error and the runner will be shut down. If it returns Ok, then
// the runner will assume that that's all the T's that will ever be generated,
// and when it's done with those T's, then it's done.
//
// Three kinds of callbacks you can install for dealing with individual T's:
//   * terminal (assumed to be blocking, will be called from a thread, and
//     will have a signature which completely consumes the Box<T>
//   * nonterminal blocking (will be called from a thread with concurrency 1,
//     will accept a Box<T> and return a Box<T>)
//   * nonterminal async (will be called from a task with the given
//     concurrency, will accept a Box<T> and return a Box<T>)
// All three of these are infallible. If something really truly catastrophic
// happens they can panic, but the expectation is that most of the time
// anything that goes wrong can be handled by setting the T to an error state.
//
// The runner can shut down, and it will be either due to an error or not.
// The first person to shut down the runner gets to decide that. Later errors
// that happen cannot change that. If the first person decides to shut down
// the runner successfully, and during shutdown an error happens, the runner
// has still shut down successfully.
//
// (The runner will shut itself down, successfully, once all the T's have
// reached a terminal state. The runner will also shut itself down,
// unsuccessfully, if any of the threads or tasks panic)

struct RunnerInsideMutex<T> {
    // None: running
    // Some(Ok): exited successfully
    // Some(Err): error
    status: Option<Result<(), crate::error::WuffError>>,
    status_event: std::sync::Arc<tokio::sync::Notify>,

    // The predicate needs to be wrapped in an Arc because it's not just the
    // RunnerInsideMutex that is going to hold on to a reference, it's also the
    // handler threads themselves. Each handler thread knows what *its own*
    // predicate is.
    //
    // The queue writer needs to be wrapped in an Arc because we have to be
    // inside the mutex while we are figuring out *which* queue something
    // belongs in, but if the queue is full and we have to block, we don't
    // want to block while holding the mutex, so we have to be able to bring
    // the queue writer outside of the mutex.
    handlers: Vec<(
        std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync>,
        std::sync::Arc<tokio::sync::mpsc::Sender<Box<T>>>,
    )>,

    // Three things need to happen before we can consider ourselves to be done:
    //   1. the feeder thread has to complete, SUCCESSFULLY (no panicking!)
    //   2. the queue the feeder thread was filling has to be emptied
    //   3. the number of in-flight objects has to drop to zero
    // Any time any of these three things changes, you should call check_done()
    // to see if we're done.
    feeder_done: bool,
    feed_queue_emptied: bool,
    in_flight: u64,
}

impl<T> RunnerInsideMutex<T> {
    fn shutdown_successfully(&mut self) {
        if self.status.is_some() {
            return;
        }
        self.status = Some(Ok(()));
        self.status_event.notify_one();

        // Just shutting down ought to be enough, but for extra safety, we
        // drop the writers, which will send EOF to the readers.
        self.handlers.clear();
    }
    fn shutdown_with_error<E>(&mut self, err: E)
    where
        E: Into<crate::error::WuffError>,
    {
        if self.status.is_some() {
            return;
        }
        self.status = Some(Err(err.into()));
        self.status_event.notify_one();

        // Just shutting down ought to be enough, but for extra safety, we
        // drop the writers, which will send EOF to the readers.
        self.handlers.clear();
    }
    fn check_done(&mut self) {
        if (self.in_flight == 0) && self.feeder_done && self.feed_queue_emptied
        {
            self.shutdown_successfully();
        }
    }
    // The sorting hat will not block, and is suitable for use in both
    // blocking and async code.
    //
    // It will try to enqueue your thing for you. If it can, then that saves
    // a refcount incr/decr on the Arc. But if it can't, it will give your
    // thing back to you, along with the queue it belongs on, and it is now
    // up to you to release the mutex and go block (or yield, if you are
    // async) for as long as you need to.
    fn sorting_hat(
        &self,
        x: Box<T>,
    ) -> Option<(Box<T>, std::sync::Arc<tokio::sync::mpsc::Sender<Box<T>>>)>
    {
        if self.status.is_some() {
            panic!("runner is shutting down");
        }

        for (pred, writer) in &self.handlers {
            if pred(x.as_ref()) {
                if let Err(try_send_err) = writer.try_send(x) {
                    // We want to panic if the queue is closed. If it's just
                    // full, that's ok...
                    match try_send_err {
                        tokio::sync::mpsc::error::TrySendError::Full(x) => {
                            return Some((x, std::sync::Arc::clone(&writer)));
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(_) => {
                            panic!("queue was closed");
                        }
                    }
                } else {
                    // Success! It was sent. The caller doesn't have to do
                    // anything.
                    return None;
                }
            }
        }
        panic!("The sorting hat never makes a mistake!");
    }

    fn check_for_shutdown(&self) {
        if self.status.is_some() {
            panic!("runner is shutting down");
        }
    }
}

pub struct Runner<T> {
    inside_mutex: std::sync::Arc<std::sync::RwLock<RunnerInsideMutex<T>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<T: Send + 'static> Runner<T> {
    pub fn new() -> Runner<T> {
        Runner {
            inside_mutex: std::sync::Arc::new(std::sync::RwLock::new(
                RunnerInsideMutex {
                    status: None,
                    status_event: std::sync::Arc::new(
                        tokio::sync::Notify::new(),
                    ),
                    handlers: Vec::new(),
                    feeder_done: false,
                    feed_queue_emptied: false,
                    in_flight: 0,
                },
            )),
            tasks: Vec::new(),
        }
    }

    pub fn handle_terminal<P, H>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        pred: P,
        mut handler: H,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: FnMut(Box<T>) + Send + 'static,
    {
        let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
            std::sync::Arc::new(pred);
        let (writer, mut reader) = tokio::sync::mpsc::channel::<Box<T>>(1000);
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn_blocking({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                move || {
                    while let Some(x) = reader.blocking_recv() {
                        handler(x);
                        let mut inside_mutex =
                            ins_mut.write().expect("Runner");
                        inside_mutex.in_flight -= 1;
                        inside_mutex.check_done();
                    }
                }
            }),
        );
        let mut inside_mutex = self.inside_mutex.write().expect("Runner");
        inside_mutex
            .handlers
            .push((pred, std::sync::Arc::new(writer)));
    }

    pub fn handle_nonterminal_blocking<P, H>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        pred: P,
        mut handler: H,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: FnMut(&mut T) + Send + 'static,
    {
        let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
            std::sync::Arc::new(pred);
        let (writer, mut reader) = tokio::sync::mpsc::channel::<Box<T>>(1000);
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn_blocking({
                let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
                    std::sync::Arc::clone(&pred);
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                move || {
                    while let Some(mut x) = reader.blocking_recv() {
                        // Careful not to deadlock. If the predicate is still
                        // true after the handler handles it, we should NOT
                        // put it through the sorting hat again, because we
                        // might be trying to put it back into our queue, and
                        // our queue might be full...
                        //
                        // So we do this in a loop, and only get the sorting
                        // hat involved once we are sure it doesn't still match
                        // the predicate.
                        loop {
                            handler(x.as_mut());
                            if !pred(x.as_ref()) {
                                break;
                            } else {
                                // we still have to acquire the lock and check
                                // if we are shutting down...
                                let inside_mutex =
                                    ins_mut.read().expect("Runner");
                                inside_mutex.check_for_shutdown();
                            }
                        }

                        if let Some((x, writer)) = {
                            let inside_mutex = ins_mut.read().expect("Runner");
                            inside_mutex.sorting_hat(x)
                        } {
                            // We're not holding the lock anymore, so
                            // we can block.
                            writer.blocking_send(x).expect("queue was closed");
                        }
                    }
                }
            }),
        );
        let mut inside_mutex = self.inside_mutex.write().expect("Runner");
        inside_mutex
            .handlers
            .push((pred, std::sync::Arc::new(writer)));
    }

    pub fn handle_nonterminal_async<P, H, F>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        pred: P,
        handler: H,
        concurrency: u16,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: Fn(&mut T) -> F + Send + Sync + 'static,
        F: std::future::Future<Output = ()> + Send,
    {
        let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
            std::sync::Arc::new(pred);

        let handler: std::sync::Arc<H> = std::sync::Arc::new(handler);

        // Careful not to deadlock. If the predicate is still true after the
        // handler handles it, we need to loop around right away and do it
        // again. If we have already moved on to do other work by the time
        // we notice the predicate is still true, we might already be at max
        // concurrency, and the queue might be full, and we would just have
        // no place to put the object. So we have to catch it right away. And
        // the way we do that is by replacing the handler with a new handler
        // that knows how to loop.
        //
        // The sheer number of Arcs involved here is ugh.
        //  - The handler replacement will get spawned as a task, so the
        //    closure portion of it needs to have an Arc that it can clone
        //    for the benefit of the async block.
        //  - The thing that will spawn the handler replacement as a task,
        //    is itself a Future which Rust thinks might outlive the current
        //    function. So it can't borrow things from the current function,
        //    it has to own Arcs that it can clone.
        let handler = {
            let pred = std::sync::Arc::clone(&pred);
            let handler: std::sync::Arc<H> = std::sync::Arc::clone(&handler);
            let ins_mut: std::sync::Arc<
                std::sync::RwLock<RunnerInsideMutex<T>>,
            > = std::sync::Arc::clone(&self.inside_mutex);
            move |mut x: Box<T>| {
                let pred = std::sync::Arc::clone(&pred);
                let handler: std::sync::Arc<H> =
                    std::sync::Arc::clone(&handler);
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&ins_mut);
                async move {
                    loop {
                        handler(x.as_mut()).await;
                        if !pred(x.as_ref()) {
                            break x;
                        }
                    }
                }
            }
        };

        let (writer, mut reader) = tokio::sync::mpsc::channel::<Box<T>>(1000);
        let bp: crate::util::BoundedParallelism<Box<T>> =
            crate::util::BoundedParallelism::new(concurrency);
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ctx: std::sync::Arc<crate::ctx::Ctx> =
                    std::sync::Arc::clone(&ctx);
                let pred = std::sync::Arc::clone(&pred);
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                async move {
                    while let Some(mut x) = reader.recv().await {
                        for res in bp.spawn(&ctx, handler(x)).await {
                            match res {
                                Ok(x) => {
                                    if let Some((x, writer)) = {
                                        let inside_mutex =
                                            ins_mut.read().expect("Runner");
                                        inside_mutex.sorting_hat(x)
                                    } {
                                        // We're not holding the lock anymore,
                                        // so we can block.
                                        writer
                                            .send(x)
                                            .await
                                            .expect("queue was closed");
                                    }
                                }
                                Err(e) => {
                                    let mut inside_mutex =
                                        ins_mut.write().expect("Runner");
                                    inside_mutex.shutdown_with_error(e);
                                }
                            }
                        }
                    }
                    for res in bp.drain().await {
                        match res {
                            Ok(x) => {
                                if let Some((x, writer)) = {
                                    let inside_mutex =
                                        ins_mut.read().expect("Runner");
                                    inside_mutex.sorting_hat(x)
                                } {
                                    // We're not holding the lock anymore,
                                    // so we can block.
                                    writer
                                        .send(x)
                                        .await
                                        .expect("queue was closed");
                                }
                            }
                            Err(e) => {
                                let mut inside_mutex =
                                    ins_mut.write().expect("Runner");
                                inside_mutex.shutdown_with_error(e);
                            }
                        }
                    }
                }
            }),
        );
        let mut inside_mutex = self.inside_mutex.write().expect("Runner");
        inside_mutex
            .handlers
            .push((pred, std::sync::Arc::new(writer)));
    }

    pub async fn run_blocking<F>(
        mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        feeder: F,
    ) -> Result<(), crate::error::WuffError>
    where
        F: FnOnce(tokio::sync::mpsc::Sender<Box<T>>) + Send + 'static,
    {
        let (writer, reader) = tokio::sync::mpsc::channel::<Box<T>>(1000);
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn_blocking({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                move || {
                    feeder(writer);
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    inside_mutex.feeder_done = true;
                    inside_mutex.check_done();
                }
            }),
        );
        self.run(ctx, reader).await
    }

    pub async fn run_async<F, FF>(
        mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        feeder: F,
    ) -> Result<(), crate::error::WuffError>
    where
        F: FnOnce(tokio::sync::mpsc::Sender<Box<T>>) -> FF + Send + 'static,
        FF: std::future::Future<Output = ()> + Send,
    {
        let (writer, reader) = tokio::sync::mpsc::channel::<Box<T>>(1000);
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                async move {
                    feeder(writer).await;
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    inside_mutex.feeder_done = true;
                    inside_mutex.check_done();
                }
            }),
        );
        self.run(ctx, reader).await
    }

    fn nanny(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        task: tokio::task::JoinHandle<()>,
    ) {
        self.tasks.push(ctx.get_async_spawner().spawn({
            let ins_mut: std::sync::Arc<
                std::sync::RwLock<RunnerInsideMutex<T>>,
            > = std::sync::Arc::clone(&self.inside_mutex);
            async move {
                if let Err(e) = task.await {
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    inside_mutex.shutdown_with_error(e);
                }
            }
        }));
    }

    async fn run(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        mut reader: tokio::sync::mpsc::Receiver<Box<T>>,
    ) -> Result<(), crate::error::WuffError> {
        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                async move {
                    while let Some(x) = reader.recv().await {
                        if let Some((x, writer)) = {
                            let mut inside_mutex =
                                ins_mut.write().expect("Runner");
                            inside_mutex.in_flight += 1;
                            // we don't have to call check_done() since we
                            // know we can't possibly be done!
                            inside_mutex.sorting_hat(x)
                        } {
                            // We're not holding the lock anymore, so
                            // we can block.
                            writer.send(x).await.expect("queue was closed");
                        }
                    }
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    inside_mutex.feed_queue_emptied = true;
                    inside_mutex.check_done();
                }
            }),
        );

        // All the threads are spawned. Each one has a nanny watching it to
        // see if it panics. (The nannies never panic.)
        //
        // We just have to wait until the whole thing finishes. To do that,
        // we will copy the Arc for the status_event, and then release the
        // lock, before waiting on that status_event.
        let status_event: std::sync::Arc<tokio::sync::Notify> = {
            let inside_mutex = self.inside_mutex.read().expect("Runner");
            std::sync::Arc::clone(&inside_mutex.status_event)
        };

        status_event.notified().await;

        let status: Result<(), crate::error::WuffError> = {
            let inside_mutex = self.inside_mutex.read().expect("Runner");
            inside_mutex.status.clone().unwrap()
        };

        // If it was a success, it's probably worth awaiting all the tasks
        // we spawned. If it was a failure, just let them fall where they may.
        if status.is_ok() {
            while let Some(task) = self.tasks.pop() {
                task.await.expect("Runner");
            }
        }

        status
    }
}

impl<T> Drop for Runner<T> {
    fn drop(&mut self) {
        let mut inside_mutex = self.inside_mutex.write().expect("Runner");
        inside_mutex.shutdown_with_error("dropped");
    }
}
