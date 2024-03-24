// Two kinds of callbacks you can install for generating Box<T>'s:
//   * blocking (will be spawned in a thread)
//   * async (will be spawned as a task)
// Either one can either return an error result, or panic. Both are treated
// as a fatal error and the runner will be shut down. If it returns Ok, then
// the runner will assume that that's all the T's that will ever be generated,
// and when it's done with those T's, then it's done.
//
// Three kinds of callbacks you can install for dealing with individual T's:
//   * terminal (blocking, will be called from a thread with concurrency 1,
//     will accept and consume a Box<T>, and return a Result)
//   * nonterminal blocking (will be called from a thread with concurrency 1,
//     will accept a &mut T)
//   * nonterminal async (will be called from a task with the given
//     concurrency, will accept a Box<T> and return a Box<T>)
// The last two of these are infallible. If something really truly catastrophic
// happens they can panic, but the expectation is that most of the time
// anything that goes wrong can be handled by setting the T to an error state,
// which is presumably a terminal state, and can be handled appropriately by
// a terminal handler.
//
// (A handler for a terminal error state gets to choose whether it stops the
// whole Runner as a result of the error, or whether it just accepts that
// that particular T failed, and moves on)
//
// The runner can shut down, and it will be either due to an error or not.
// The first person to shut down the runner gets to decide that. Later errors
// that happen cannot change that. If the first person decides to shut down
// the runner successfully, and during shutdown an error happens, the runner
// has still shut down successfully.
//
// (The runner will shut itself down, successfully, once all the T's have
// reached a terminal state. The runner will also shut itself down,
// unsuccessfully, if any of the threads or tasks panic, or if a feeder or a
// terminal handler returns an error Result)

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
    //   1. the feeder thread has to complete, SUCCESSFULLY
    //   2. the queue the feeder thread was filling has to be emptied
    //   3. the number of in-flight objects has to drop to zero
    // Any time any of these three things changes, you should call check_done()
    // to see if we're done.
    feeder_done: bool,
    feed_queue_emptied: bool,
    in_flight: u64,

    // If you need it, here's a specially constructed queue writer which will
    // always return an error if you try to enqueue anything onto it.
    //
    // (May you never need it.)
    bad_writer: std::sync::Arc<tokio::sync::mpsc::Sender<Box<T>>>,
}

impl<T> RunnerInsideMutex<T> {
    fn shutdown_successfully(&mut self) {
        if self.status.is_some() {
            return;
        }
        self.status = Some(Ok(()));
        self.status_event.notify_one();

        // Drop the writers, which will send EOF to the readers
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

        // Drop the writers, which will send EOF to the readers
        self.handlers.clear();
    }

    fn check_done(&mut self) {
        if self.feeder_done && self.feed_queue_emptied && (self.in_flight == 0)
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
            // We would like to panic. But we can't panic, because we're
            // holding the lock, and panicking would poison it. So we pull
            // the pin out of a grenade, and we calmly hand it to our
            // caller.
            return Some((x, std::sync::Arc::clone(&self.bad_writer)));
        }

        for (pred, writer) in &self.handlers {
            if pred(x.as_ref()) {
                if let Err(try_send_err) = writer.try_send(x) {
                    // If it's full, that's fine, let the caller block, If
                    // it's closed, that's also fine, let the caller panic.
                    // We can't stop here, this is bat country.
                    match try_send_err {
                        tokio::sync::mpsc::error::TrySendError::Full(x) => {
                            return Some((x, std::sync::Arc::clone(&writer)));
                        }
                        tokio::sync::mpsc::error::TrySendError::Closed(x) => {
                            // lol
                            return Some((x, std::sync::Arc::clone(&writer)));
                        }
                    }
                } else {
                    // Success! It was sent. The caller doesn't have to do
                    // anything.
                    return None;
                }
            }
        }

        // this will poison us, but, well, it's true, the sorting hat doesn't
        // ever make a mistake!
        panic!("The sorting hat never makes a mistake!");
    }
}

pub struct Runner<T> {
    inside_mutex: std::sync::Arc<std::sync::RwLock<RunnerInsideMutex<T>>>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl<T: Send + 'static> Runner<T> {
    pub fn new() -> Runner<T> {
        let (bad_writer, _) = tokio::sync::mpsc::channel::<Box<T>>(1);
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
                    bad_writer: std::sync::Arc::new(bad_writer),
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
        queue_size: usize,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: FnMut(Box<T>) -> Result<(), crate::error::WuffError>
            + Send
            + 'static,
    {
        let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
            std::sync::Arc::new(pred);
        let (writer, mut reader) =
            tokio::sync::mpsc::channel::<Box<T>>(queue_size);

        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn_blocking({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                move || {
                    while let Some(x) = reader.blocking_recv() {
                        let result: Result<(), crate::error::WuffError> =
                            handler(x);
                        let mut inside_mutex =
                            ins_mut.write().expect("Runner");
                        if let Err(e) = result {
                            inside_mutex.shutdown_with_error(e);
                        } else {
                            inside_mutex.in_flight -= 1;
                            inside_mutex.check_done();
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

    pub fn handle_nonterminal_blocking<P, H>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        pred: P,
        mut handler: H,
        queue_size: usize,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: FnMut(&mut T) + Send + 'static,
    {
        let pred: std::sync::Arc<dyn Fn(&T) -> bool + Send + Sync> =
            std::sync::Arc::new(pred);
        let (writer, mut reader) =
            tokio::sync::mpsc::channel::<Box<T>>(queue_size);

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
                            } else if {
                                let inside_mutex =
                                    ins_mut.read().expect("Runner");
                                inside_mutex.status.is_some()
                            } {
                                panic!("runner is shutting down");
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

    // Couple notes:
    //
    //   1. The handler is Fn, not FnMut, because we support concurrency.
    //      If there's ever a need for a version of this that allows FnMut,
    //      but doesn't allow more than one invocation of the handler to be
    //      in-flight at the same time, that's ok, that can be added.
    //
    //   2. The callback really should be:
    //        handler: H where
    //          H: Fn(&mut T) -> F + Send + Sync + 'static,
    //          F: std::future::Future<Output = ()> + Send,
    //      but that causes problems because of the reference. Remember that
    //      an async function that is in the process of executing, is
    //      represented as a Future, which is just a struct, and so that
    //      struct is gonna have to be holding that reference, and that might
    //      even work, normally, but we're doing enough weird stuff here that
    //      the lifetimes are a gordian knot.
    //
    //      So we make the handlers take a Box, and return a Box.
    pub fn handle_nonterminal_async<P, H, F>(
        &mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        pred: P,
        handler: H,
        queue_size: usize,
        concurrency: u16,
    ) where
        P: Fn(&T) -> bool + Send + Sync + 'static,
        H: Fn(Box<T>) -> F + Send + Sync + 'static,
        F: std::future::Future<Output = Box<T>> + Send,
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
                        x = handler(x).await;
                        if !pred(x.as_ref()) {
                            break x;
                        } else if {
                            let inside_mutex = ins_mut.read().expect("Runner");
                            inside_mutex.status.is_some()
                        } {
                            panic!("runner is shutting down");
                        }
                    }
                }
            }
        };

        let (writer, mut reader) =
            tokio::sync::mpsc::channel::<Box<T>>(queue_size);
        let bp: std::sync::Arc<crate::util::BoundedParallelism<Box<T>>> =
            std::sync::Arc::new(crate::util::BoundedParallelism::new(
                concurrency,
            ));

        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ctx: std::sync::Arc<crate::ctx::Ctx> =
                    std::sync::Arc::clone(&ctx);
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                let bp: std::sync::Arc<
                    crate::util::BoundedParallelism<Box<T>>,
                > = std::sync::Arc::clone(&bp);
                async move {
                    while let Some(x) = reader.recv().await {
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
                }
            }),
        );

        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                let bp: std::sync::Arc<
                    crate::util::BoundedParallelism<Box<T>>,
                > = std::sync::Arc::clone(&bp);
                async move {
                    loop {
                        // 1. Wait a little bit
                        tokio::time::sleep(std::time::Duration::from_millis(
                            100,
                        ))
                        .await;

                        // 2. Check if we're supposed to exit
                        if {
                            let inside_mutex = ins_mut.read().expect("Runner");
                            inside_mutex.status.is_some()
                        } {
                            break;
                        }

                        // 3. Collect any stragglers
                        for res in bp.collect() {
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
        queue_size: usize,
    ) -> Result<(), crate::error::WuffError>
    where
        F: FnOnce(
                tokio::sync::mpsc::Sender<Box<T>>,
            ) -> Result<(), crate::error::WuffError>
            + Send
            + 'static,
    {
        let (writer, reader) =
            tokio::sync::mpsc::channel::<Box<T>>(queue_size);

        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn_blocking({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                move || {
                    let result: Result<(), crate::error::WuffError> =
                        feeder(writer);
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    if let Err(err) = result {
                        inside_mutex.shutdown_with_error(err);
                    } else {
                        inside_mutex.feeder_done = true;
                        inside_mutex.check_done();
                    }
                }
            }),
        );

        self.run(ctx, reader).await
    }

    pub async fn run_async<F, FF>(
        mut self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        feeder: F,
        queue_size: usize,
    ) -> Result<(), crate::error::WuffError>
    where
        F: FnOnce(tokio::sync::mpsc::Sender<Box<T>>) -> FF + Send + 'static,
        FF: std::future::Future<Output = Result<(), crate::error::WuffError>>
            + Send,
    {
        let (writer, reader) =
            tokio::sync::mpsc::channel::<Box<T>>(queue_size);

        self.nanny(
            ctx,
            ctx.get_async_spawner().spawn({
                let ins_mut: std::sync::Arc<
                    std::sync::RwLock<RunnerInsideMutex<T>>,
                > = std::sync::Arc::clone(&self.inside_mutex);
                async move {
                    let result: Result<(), crate::error::WuffError> =
                        feeder(writer).await;
                    let mut inside_mutex = ins_mut.write().expect("Runner");
                    if let Err(err) = result {
                        inside_mutex.shutdown_with_error(err);
                    } else {
                        inside_mutex.feeder_done = true;
                        inside_mutex.check_done();
                    }
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

///////////////////////////////////////////////////////////////////////////////
///////////// EVERYTHING BELOW THIS LINE IS RELATED TO UNIT TESTS /////////////
///////////////////////////////////////////////////////////////////////////////

#[cfg(test)]
struct TestStateMachineManager {
    num: usize,
    done: std::sync::Mutex<Vec<bool>>,
    state_transitions: std::sync::Mutex<Vec<u8>>,
}
#[cfg(test)]
impl TestStateMachineManager {
    fn new(num: usize) -> std::sync::Arc<Self> {
        let mut done: Vec<bool> = Vec::with_capacity(num);
        done.resize(num, false);
        let mut state_transitions: Vec<u8> = Vec::with_capacity(num);
        state_transitions.resize(num, 0u8);
        std::sync::Arc::new(Self {
            num: num,
            done: std::sync::Mutex::new(done),
            state_transitions: std::sync::Mutex::new(state_transitions),
        })
    }

    fn install_for_success(
        &self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        runner: &mut Runner<TestStateMachine>,
        queue_size: usize,
        async_concurrency: u16,
    ) {
        runner.handle_terminal(
            &ctx,
            TestStateMachine::pred_terminal,
            TestStateMachine::handle_terminal,
            queue_size,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_1,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_2,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_3,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_4,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_5,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_6,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_7,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_8,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
    }

    fn install_with_terminal_that_errors(
        &self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        runner: &mut Runner<TestStateMachine>,
        queue_size: usize,
        async_concurrency: u16,
    ) {
        runner.handle_terminal(
            &ctx,
            TestStateMachine::pred_terminal,
            TestStateMachine::handle_terminal_err,
            queue_size,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_1,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_2,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_3,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_4,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_5,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_6,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_7,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_8,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
    }

    fn install_with_one_blocking_that_panics(
        &self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        runner: &mut Runner<TestStateMachine>,
        queue_size: usize,
        async_concurrency: u16,
    ) {
        runner.handle_terminal(
            &ctx,
            TestStateMachine::pred_terminal,
            TestStateMachine::handle_terminal,
            queue_size,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_1,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_2,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_3,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_4,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_5,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_6,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_7,
            TestStateMachine::handle_nonterminal_blocking_panic,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_8,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
    }

    fn install_with_one_async_that_panics(
        &self,
        ctx: &std::sync::Arc<crate::ctx::Ctx>,
        runner: &mut Runner<TestStateMachine>,
        queue_size: usize,
        async_concurrency: u16,
    ) {
        runner.handle_terminal(
            &ctx,
            TestStateMachine::pred_terminal,
            TestStateMachine::handle_terminal,
            queue_size,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_1,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_2,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_3,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_4,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_5,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_6,
            TestStateMachine::handle_nonterminal_async,
            queue_size,
            async_concurrency,
        );
        runner.handle_nonterminal_blocking(
            &ctx,
            TestStateMachine::pred_nonterminal_blocking_7,
            TestStateMachine::handle_nonterminal_blocking,
            queue_size,
        );
        runner.handle_nonterminal_async(
            &ctx,
            TestStateMachine::pred_nonterminal_async_8,
            TestStateMachine::handle_nonterminal_async_panic,
            queue_size,
            async_concurrency,
        );
    }

    fn rand_feeder_blocking(
        self: std::sync::Arc<Self>,
        writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>,
    ) -> Result<(), crate::error::WuffError> {
        let mut rng: rand::rngs::ThreadRng = rand::thread_rng();
        for i in 0usize..self.num {
            writer
                .blocking_send(Box::new(TestStateMachine {
                    id: i,
                    state: rand::RngCore::next_u32(&mut rng) as u8,
                    mgr: std::sync::Arc::clone(&self),
                }))
                .unwrap();
        }
        Ok(())
    }

    async fn rand_feeder_async(
        self: std::sync::Arc<Self>,
        writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>,
    ) -> Result<(), crate::error::WuffError> {
        for i in 0usize..self.num {
            // rand::rngs::ThreadRng is not Send, and it can't be held across
            // an await. That means we have to get all the randomness we need,
            // up front, store it in variables, and then drop the ThreadRng.
            let state: u8 = {
                let mut rng: rand::rngs::ThreadRng = rand::thread_rng();
                rand::RngCore::next_u32(&mut rng) as u8
            };
            writer
                .send(Box::new(TestStateMachine {
                    id: i,
                    state: state,
                    mgr: std::sync::Arc::clone(&self),
                }))
                .await
                .unwrap();
        }
        Ok(())
    }

    fn deterministic_feeder_blocking(
        self: std::sync::Arc<Self>,
        writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>,
    ) -> Result<(), crate::error::WuffError> {
        for i in 0usize..self.num {
            writer
                .blocking_send(Box::new(TestStateMachine {
                    id: i,
                    state: 0xffu8,
                    mgr: std::sync::Arc::clone(&self),
                }))
                .unwrap();
        }
        Ok(())
    }

    fn deterministic_feeder_blocking_err(
        self: std::sync::Arc<Self>,
        writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>,
    ) -> Result<(), crate::error::WuffError> {
        for i in 0usize..self.num {
            writer
                .blocking_send(Box::new(TestStateMachine {
                    id: i,
                    state: 0xffu8,
                    mgr: std::sync::Arc::clone(&self),
                }))
                .unwrap();
        }
        Err("Snyarf, snyarf, snyarf!".into())
    }

    async fn deterministic_feeder_async_err(
        self: std::sync::Arc<Self>,
        writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>,
    ) -> Result<(), crate::error::WuffError> {
        for i in 0usize..self.num {
            writer
                .send(Box::new(TestStateMachine {
                    id: i,
                    state: 0xffu8,
                    mgr: std::sync::Arc::clone(&self),
                }))
                .await
                .unwrap();
        }
        Err("Snyarf, snyarf, snyarf!".into())
    }
}

#[cfg(test)]
struct TestStateMachine {
    id: usize,
    state: u8,
    mgr: std::sync::Arc<TestStateMachineManager>,
}

#[cfg(test)]
impl TestStateMachine {
    fn handle_terminal(
        self: Box<Self>,
    ) -> Result<(), crate::error::WuffError> {
        let mut done = self.mgr.done.lock().unwrap();
        done[self.id] = true;
        Ok(())
    }
    fn handle_terminal_err(
        self: Box<Self>,
    ) -> Result<(), crate::error::WuffError> {
        if self.id == 5000 {
            Err("oh hi".into())
        } else {
            let mut done = self.mgr.done.lock().unwrap();
            done[self.id] = true;
            Ok(())
        }
    }
    fn pred_terminal(&self) -> bool {
        self.state == 0
    }

    fn handle_nonterminal_blocking(&mut self) {
        self.advance_state();
    }
    fn handle_nonterminal_blocking_panic(&mut self) {
        if self.id == 5000 {
            panic!("at the disco");
        } else {
            self.advance_state();
        }
    }
    async fn handle_nonterminal_async(mut self: Box<Self>) -> Box<Self> {
        for _ in 0..3 {
            tokio::time::sleep(std::time::Duration::from_nanos(1)).await;
        }
        self.advance_state();
        self
    }
    async fn handle_nonterminal_async_panic(mut self: Box<Self>) -> Box<Self> {
        if self.id == 5000 {
            panic!("at the disco");
        } else {
            for _ in 0..3 {
                tokio::time::sleep(std::time::Duration::from_nanos(1)).await;
            }
            self.advance_state();
            self
        }
    }
    fn pred_nonterminal_blocking_1(&self) -> bool {
        (self.state & 0x80) == 0x80
    }
    fn pred_nonterminal_async_2(&self) -> bool {
        (self.state & 0xc0) == 0x40
    }
    fn pred_nonterminal_blocking_3(&self) -> bool {
        (self.state & 0xe0) == 0x20
    }
    fn pred_nonterminal_async_4(&self) -> bool {
        (self.state & 0xf0) == 0x10
    }
    fn pred_nonterminal_blocking_5(&self) -> bool {
        (self.state & 0xf8) == 0x08
    }
    fn pred_nonterminal_async_6(&self) -> bool {
        (self.state & 0xfc) == 0x04
    }
    fn pred_nonterminal_blocking_7(&self) -> bool {
        (self.state & 0xfe) == 0x02
    }
    fn pred_nonterminal_async_8(&self) -> bool {
        (self.state & 0xff) == 0x01
    }

    fn advance_state(&mut self) {
        // Find the highest bit that is 1, and clear it
        if (self.state & 0x80) != 0 {
            self.state &= 0x7f;
        } else if (self.state & 0x40) != 0 {
            self.state &= 0x3f;
        } else if (self.state & 0x20) != 0 {
            self.state &= 0x1f;
        } else if (self.state & 0x10) != 0 {
            self.state &= 0x0f;
        } else if (self.state & 0x08) != 0 {
            self.state &= 0x07;
        } else if (self.state & 0x04) != 0 {
            self.state &= 0x03;
        } else if (self.state & 0x02) != 0 {
            self.state &= 0x01;
        } else {
            self.state = 0;
        }
        let mut state_transitions = self.mgr.state_transitions.lock().unwrap();
        state_transitions[self.id] += 1;
    }
}

#[test]
fn test_runner_success_with_blocking_feeder() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_for_success(&ctx, &mut runner, 100, 10);
        runner
            .run_blocking(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<
                        Box<TestStateMachine>,
                    >| { mgr.rand_feeder_blocking(writer) }
                },
                100,
            )
            .await
            .expect("should have succeeded");
        let done = mgr.done.lock().unwrap();
        let state_transitions = mgr.state_transitions.lock().unwrap();
        for x in std::ops::Deref::deref(&done) {
            assert!(*x);
        }
        let mut total: u32 = 0;
        for x in std::ops::Deref::deref(&state_transitions) {
            total += *x as u32;
        }
        println!("Ran {} state machines", state_transitions.len());
        println!(
            "Average number of state transitions: {}",
            (total as f32) / (state_transitions.len() as f32)
        );
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_success_with_async_feeder() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_for_success(&ctx, &mut runner, 100, 10);
        runner
            .run_async(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<
                        Box<TestStateMachine>,
                    >| { mgr.rand_feeder_async(writer) }
                },
                100,
            )
            .await
            .expect("should have succeeded");
        let done = mgr.done.lock().unwrap();
        let state_transitions = mgr.state_transitions.lock().unwrap();
        for x in std::ops::Deref::deref(&done) {
            assert!(*x);
        }
        let mut total: u32 = 0;
        for x in std::ops::Deref::deref(&state_transitions) {
            total += *x as u32;
        }
        println!("Ran {} state machines", state_transitions.len());
        println!(
            "Average number of state transitions: {}",
            (total as f32) / (state_transitions.len() as f32)
        );
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_with_panic_in_blocking_handler() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_with_one_blocking_that_panics(&ctx, &mut runner, 100, 10);
        let result = runner
            .run_blocking(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>| -> Result<(), crate::error::WuffError> { mgr.deterministic_feeder_blocking(writer) }
                },
                100,
            )
            .await;
        assert!(result.is_err());
        let done = mgr.done.lock().unwrap();
        let mut total: u16 = 0;
        for x in std::ops::Deref::deref(&done) {
            if *x {
                total += 1;
            }
        }
        println!("Ran {} state machines", done.len());
        println!("{} finished before error", total);
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_with_panic_in_async_handler() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_with_one_async_that_panics(&ctx, &mut runner, 100, 10);
        let result = runner
            .run_blocking(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>| -> Result<(), crate::error::WuffError> { mgr.deterministic_feeder_blocking(writer) }
                },
                100,
            )
            .await;
        assert!(result.is_err());
        let done = mgr.done.lock().unwrap();
        let mut total: u16 = 0;
        for x in std::ops::Deref::deref(&done) {
            if *x {
                total += 1;
            }
        }
        println!("Ran {} state machines", done.len());
        println!("{} finished before error", total);
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_with_err_in_terminal_handler() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_with_terminal_that_errors(&ctx, &mut runner, 100, 10);
        let result = runner
            .run_blocking(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>| -> Result<(), crate::error::WuffError> { mgr.deterministic_feeder_blocking(writer) }
                },
                100,
            )
            .await;
        assert!(result.is_err());
        let done = mgr.done.lock().unwrap();
        let mut total: u16 = 0;
        for x in std::ops::Deref::deref(&done) {
            if *x {
                total += 1;
            }
        }
        println!("Ran {} state machines", done.len());
        println!("{} finished before error", total);
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_with_err_in_blocking_feeder() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_for_success(&ctx, &mut runner, 100, 10);
        let result = runner
            .run_blocking(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<Box<TestStateMachine>>| -> Result<(), crate::error::WuffError> { mgr.deterministic_feeder_blocking_err(writer) }
                },
                100,
            )
            .await;
        assert!(result.is_err());
        let done = mgr.done.lock().unwrap();
        let mut total: u16 = 0;
        for x in std::ops::Deref::deref(&done) {
            if *x {
                total += 1;
            }
        }
        println!("Ran {} state machines", done.len());
        println!("{} finished before error", total);
        Ok(())
    })
    .expect("unexpected error");
}

#[test]
fn test_runner_with_err_in_async_feeder() {
    let ctx = std::sync::Arc::new(crate::ctx::Ctx::new_minimal());
    ctx.run_async_main(async {
        let mgr = TestStateMachineManager::new(10000);
        let mut runner: Runner<TestStateMachine> = Runner::new();
        mgr.install_for_success(&ctx, &mut runner, 100, 10);
        let result = runner
            .run_async(
                &ctx,
                {
                    let mgr = std::sync::Arc::clone(&mgr);
                    move |writer: tokio::sync::mpsc::Sender<
                        Box<TestStateMachine>,
                    >| {
                        mgr.deterministic_feeder_async_err(writer)
                    }
                },
                100,
            )
            .await;
        assert!(result.is_err());
        let done = mgr.done.lock().unwrap();
        let mut total: u16 = 0;
        for x in std::ops::Deref::deref(&done) {
            if *x {
                total += 1;
            }
        }
        println!("Ran {} state machines", done.len());
        println!("{} finished before error", total);
        Ok(())
    })
    .expect("unexpected error");
}
