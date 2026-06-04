use super::Result;
use crate::stats::generate_tid;
use crate::{
    periodic::PeriodicJob, Chain, Counter, Job, RedisPool, ReliableClaim, Scheduled,
    ServerMiddleware, StatsPublisher, UnitOfWork, Worker, WorkerRef,
};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::select;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info};

/// Redis SET (pre-namespace) listing every reliable-fetch in-progress list
/// currently in use, so a surviving or restarted process can find the lists
/// belonging to dead processes and requeue them. Members are the pre-namespace
/// in-progress list keys (`queue:<q>:inprogress:<identity>`).
const WORKING_SET: &str = "reliable_fetch:working";

#[derive(Clone, Eq, PartialEq, Debug)]
pub enum WorkFetcher {
    NoWorkFound,
    Done,
}

#[derive(Clone)]
pub struct Processor {
    redis: RedisPool,
    queues: VecDeque<String>,
    human_readable_queues: Vec<String>,
    periodic_jobs: Vec<PeriodicJob>,
    workers: BTreeMap<String, Arc<WorkerRef>>,
    chain: Chain,
    busy_jobs: Counter,
    cancellation_token: CancellationToken,
    config: ProcessorConfig,
    // Sidekiq-web WorkSet bookkeeping. Both are assigned per-worker by `run()`;
    // when unset (e.g. a bare `process_one()` call), WorkSet publishing is
    // skipped. `identity` is the shared process identity (the same one the
    // heartbeat registers in `processes`); `tid` is this worker's thread id.
    identity: Option<String>,
    tid: Option<String>,
    // Reliable-fetch only: in-progress list keys this worker has already
    // recorded in the `WORKING_SET` registry, so the `SADD` runs once per
    // (queue, identity) rather than on every claim. Per-clone (each `run()`
    // worker gets its own); the `SADD` is idempotent so a missed dedupe is
    // harmless.
    registered_inprogress: HashSet<String>,
}

#[derive(Clone)]
#[non_exhaustive]
pub struct ProcessorConfig {
    /// The number of Sidekiq workers that can run at the same time. Adjust as needed based on
    /// your workload and resource (cpu/memory/etc) usage.
    ///
    /// This config value controls how many workers are spawned to handle the queues provided
    /// to [`Processor::new`]. These workers will be shared across all of these queues.
    ///
    /// If your workload is largely CPU-bound (computationally expensive), this should probably
    /// match your CPU count. This is the default.
    ///
    /// If your workload is largely IO-bound (e.g. reading from a DB, making web requests and
    /// waiting for responses, etc), this can probably be quite a bit higher than your CPU count.
    pub num_workers: usize,

    /// The strategy for balancing the priority of fetching queues' jobs from Redis. Defaults
    /// to [`BalanceStrategy::RoundRobin`].
    ///
    /// The Redis API used to fetch jobs ([brpop](https://redis.io/docs/latest/commands/brpop/))
    /// checks queues for jobs in the order the queues are provided. This means that if the first
    /// queue in the list provided to [`Processor::new`] always has an item, the other queues
    /// will never have their jobs run. To mitigate this, a [`BalanceStrategy`] can be provided
    /// to allow ensuring that no queue is starved indefinitely.
    pub balance_strategy: BalanceStrategy,

    /// Queue-specific configurations. The queues specified in this field do not need to match
    /// the list of queues provided to [`Processor::new`].
    pub queue_configs: BTreeMap<String, QueueConfig>,

    /// When `true`, jobs are fetched with *reliable* semantics: each job is
    /// claimed with `RPOPLPUSH`/`BRPOPLPUSH` into a per-process in-progress
    /// list (`queue:<q>:inprogress:<identity>`) instead of plain `BRPOP`, and
    /// a background sweep requeues the in-progress lists of processes that have
    /// died. This makes a job that is in-flight when the process is restarted
    /// or killed survive (it is re-run after recovery) instead of being lost —
    /// the equivalent of Sidekiq Pro's `super_fetch`. Delivery becomes
    /// at-least-once, so workers should be idempotent.
    ///
    /// Recovery judges a process dead by the absence of its heartbeat hash
    /// (`EXISTS <identity>`, a 60s TTL refreshed every 5s). This makes the
    /// heartbeat operationally load-bearing: if a *live* process's beat is
    /// starved past the 60s TTL, a sibling will judge it dead and requeue its
    /// in-flight jobs, running them a second time. Because each worker holds a
    /// pooled connection for its blocking `BRPOPLPUSH`, size the Redis pool to
    /// at least `num_workers` + headroom so the heartbeat publish never blocks
    /// on connection acquisition. The 12× margin (60s TTL / 5s beat) makes this
    /// unlikely, but it is the main new operational dependency of this mode.
    ///
    /// Defaults to `false`, preserving the original at-most-once `BRPOP` fetch.
    pub reliable: bool,

    /// On shutdown (the [`Processor::get_cancellation_token`] is cancelled,
    /// e.g. from a SIGTERM handler), how long an in-flight job is given to
    /// finish before it is interrupted and pushed back onto its queue for a
    /// surviving/replacement process to run. This is the Rust equivalent of
    /// Ruby Sidekiq's shutdown `timeout` + `BasicFetch#bulk_requeue`: jobs
    /// that finish within the window are acked normally; those that don't
    /// (e.g. a long backfill) are re-queued (at-least-once) rather than lost
    /// to the imminent process exit. Set it comfortably below your
    /// orchestrator's kill grace (e.g. k8s `terminationGracePeriodSeconds`) so
    /// the requeue completes before SIGKILL. Defaults to 25s (Ruby Sidekiq's
    /// default `-t`).
    pub shutdown_grace: Duration,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub enum BalanceStrategy {
    /// Rotate the list of queues by 1 every time jobs are fetched from Redis. This allows each
    /// queue in the list to have an equal opportunity to have its jobs run.
    #[default]
    RoundRobin,
    /// Do not modify the list of queues. Warning: This can lead to queue starvation! For example,
    /// if the first queue in the list provided to [`Processor::new`] is heavily used and always
    /// has a job available to run, then the jobs in the other queues will never run.
    None,
}

#[derive(Default, Clone)]
#[non_exhaustive]
pub struct QueueConfig {
    /// Similar to `ProcessorConfig#num_workers`, except allows configuring the number of
    /// additional workers to dedicate to a specific queue. If provided, `num_workers` additional
    /// workers will be created for this specific queue.
    pub num_workers: usize,
}

impl ProcessorConfig {
    #[must_use]
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }

    #[must_use]
    pub fn balance_strategy(mut self, balance_strategy: BalanceStrategy) -> Self {
        self.balance_strategy = balance_strategy;
        self
    }

    #[must_use]
    pub fn queue_config(mut self, queue: String, config: QueueConfig) -> Self {
        self.queue_configs.insert(queue, config);
        self
    }

    /// Enable reliable fetch (see [`ProcessorConfig::reliable`]).
    #[must_use]
    pub fn reliable(mut self, reliable: bool) -> Self {
        self.reliable = reliable;
        self
    }

    /// Set the shutdown grace window (see [`ProcessorConfig::shutdown_grace`]).
    #[must_use]
    pub fn shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }
}

impl Default for ProcessorConfig {
    fn default() -> Self {
        Self {
            num_workers: num_cpus::get(),
            balance_strategy: Default::default(),
            queue_configs: Default::default(),
            reliable: false,
            shutdown_grace: Duration::from_secs(25),
        }
    }
}

impl QueueConfig {
    #[must_use]
    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = num_workers;
        self
    }
}

impl Processor {
    #[must_use]
    pub fn new(redis: RedisPool, queues: Vec<String>) -> Self {
        let busy_jobs = Counter::new(0);

        Self {
            chain: Chain::new_with_stats(busy_jobs.clone()),
            workers: BTreeMap::new(),
            periodic_jobs: vec![],
            busy_jobs,

            redis,
            queues: queues
                .iter()
                .map(|queue| format!("queue:{queue}"))
                .collect(),
            human_readable_queues: queues,
            cancellation_token: CancellationToken::new(),
            config: Default::default(),
            identity: None,
            tid: None,
            registered_inprogress: HashSet::new(),
        }
    }

    pub fn with_config(mut self, config: ProcessorConfig) -> Self {
        self.config = config;
        self
    }

    pub async fn fetch(&mut self) -> Result<Option<UnitOfWork>> {
        // Reliable fetch needs the process identity (to name its per-process
        // in-progress list). It's assigned by `run()`; if absent (e.g. a bare
        // `process_one()` outside `run()`), fall back to the plain BRPOP path
        // rather than silently doing nothing.
        if self.config.reliable {
            if let Some(identity) = self.identity.clone() {
                return self.fetch_reliable(&identity).await;
            }
        }
        self.fetch_brpop().await
    }

    /// Original at-most-once fetch: a single blocking `BRPOP` across all
    /// queues. The job leaves Redis the instant it's popped, so a crash before
    /// it finishes loses it.
    async fn fetch_brpop(&mut self) -> Result<Option<UnitOfWork>> {
        self.run_balance_strategy();

        let response: Option<(String, String)> = self
            .redis
            .get()
            .await?
            .brpop(self.queues.clone().into(), 2)
            .await?;

        if let Some((queue, job_raw)) = response {
            let job: Job = serde_json::from_str(&job_raw)?;
            return Ok(Some(UnitOfWork {
                queue,
                job,
                reliable: None,
            }));
        }

        Ok(None)
    }

    /// Reliable fetch: claim a job by atomically moving it into this process's
    /// per-process in-progress list (`queue:<q>:inprogress:<identity>`), so it
    /// survives a crash and can be requeued by [`recover_orphaned_work`].
    ///
    /// First a non-blocking `RPOPLPUSH` sweep across all queues in priority
    /// order (stopping at the first hit — a pipelined sweep would strand the
    /// extra pops). If every queue is empty, block on the rotated head queue
    /// with `BRPOPLPUSH` so we don't busy-loop; the other queues are re-checked
    /// by the next tick's sweep (≤2s later).
    async fn fetch_reliable(&mut self, identity: &str) -> Result<Option<UnitOfWork>> {
        self.run_balance_strategy();
        let queues: Vec<String> = self.queues.iter().cloned().collect();

        // (queue_key, inprogress_key, job_raw) for the first claim, if any.
        let claim: Option<(String, String, String)> = {
            let mut conn = self.redis.get().await?;
            let mut found = None;
            for queue_key in &queues {
                let inprogress = Self::inprogress_key(queue_key, identity);
                if let Some(job_raw) = conn
                    .rpoplpush(queue_key.clone(), inprogress.clone())
                    .await?
                {
                    found = Some((queue_key.clone(), inprogress, job_raw));
                    break;
                }
            }
            if found.is_none() {
                if let Some(head) = queues.first() {
                    let inprogress = Self::inprogress_key(head, identity);
                    if let Some(job_raw) =
                        conn.brpoplpush(head.clone(), inprogress.clone(), 2).await?
                    {
                        found = Some((head.clone(), inprogress, job_raw));
                    }
                }
            }
            found
        };

        let Some((queue, inprogress, job_raw)) = claim else {
            return Ok(None);
        };

        // Record this in-progress list in the registry (once per worker) so
        // orphan recovery can find and requeue it if this process dies. Best
        // effort: on failure the job is still claimed and will run; we just
        // roll back the dedupe flag so a later tick retries the registration.
        if self.registered_inprogress.insert(inprogress.clone()) {
            let registered: Result<()> = async {
                let mut conn = self.redis.get().await?;
                conn.sadd(WORKING_SET.to_string(), inprogress.clone())
                    .await?;
                Ok(())
            }
            .await;
            if let Err(err) = registered {
                error!(
                    inprogress = %inprogress,
                    "reliable fetch: failed to register in-progress list: {:?}",
                    err
                );
                self.registered_inprogress.remove(&inprogress);
            }
        }

        let job: Job = serde_json::from_str(&job_raw)?;
        Ok(Some(UnitOfWork {
            queue,
            job,
            reliable: Some(ReliableClaim {
                inprogress_key: inprogress,
                job_raw,
            }),
        }))
    }

    /// Per-process in-progress list key for a queue. `queue_key` is already the
    /// `queue:<name>` form; appending the owner identity means a process only
    /// ever drains its own claims and orphan recovery can attribute a stranded
    /// list to the process that died.
    fn inprogress_key(queue_key: &str, identity: &str) -> String {
        format!("{queue_key}:inprogress:{identity}")
    }

    /// Re-order the `Processor#queues` based on the `ProcessorConfig#balance_strategy`.
    fn run_balance_strategy(&mut self) {
        if self.queues.is_empty() {
            return;
        }

        match self.config.balance_strategy {
            BalanceStrategy::RoundRobin => self.queues.rotate_right(1),
            BalanceStrategy::None => {}
        }
    }

    pub async fn process_one(&mut self) -> Result<()> {
        loop {
            if self.cancellation_token.is_cancelled() {
                return Ok(());
            }

            if let WorkFetcher::NoWorkFound = self.process_one_tick_once().await? {
                continue;
            }

            return Ok(());
        }
    }

    pub async fn process_one_tick_once(&mut self) -> Result<WorkFetcher> {
        let work = self.fetch().await?;

        if work.is_none() {
            // If there is no job to handle, we need to add a `yield_now` in order to allow tokio's
            // scheduler to wake up another task that may be waiting to acquire a connection from
            // the Redis connection pool. See the following issue for more details:
            // https://github.com/film42/sidekiq-rs/issues/43
            tokio::task::yield_now().await;
            return Ok(WorkFetcher::NoWorkFound);
        }
        let work = work.expect("polled and found some work");

        let started = std::time::Instant::now();

        info!({
            "status" = "start",
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "jid" = &work.job.jid
        }, "sidekiq");

        let worker = if let Some(worker) = self.workers.get(&work.job.class) {
            worker.clone()
        } else {
            Arc::new(WorkerRef::not_found(work.job.class.clone()))
        };

        // Publish this job to the Sidekiq WorkSet (`<identity>:work`) so it shows
        // on the web "Busy" page, then clear it whether the job succeeds or fails.
        self.set_work(&work).await;

        // Run the job. If shutdown is signalled (the cancellation token fires,
        // e.g. from a SIGTERM handler) while the job is in flight, give it
        // `shutdown_grace` to finish; if it doesn't, interrupt it and push it
        // back onto its queue so a surviving/replacement process runs it —
        // Ruby Sidekiq's shutdown `timeout` + `BasicFetch#bulk_requeue`. Without
        // this a long job (e.g. a backfill) outlives the OS kill grace and is
        // lost. `None` ⇒ interrupted-and-requeued (don't ack/log "done").
        let cancel = self.cancellation_token.clone();
        let grace = self.config.shutdown_grace;
        let outcome: Option<Result<()>> = {
            let job_fut = self.chain.call(&work.job, worker, self.redis.clone());
            tokio::pin!(job_fut);
            tokio::select! {
                biased;
                r = &mut job_fut => Some(r),
                // Shutdown signalled: let the job finish within the grace
                // window. `timeout(..).ok()` is `Some(result)` if it finished,
                // `None` (Elapsed) if it must be interrupted and requeued.
                () = cancel.cancelled() => tokio::time::timeout(grace, &mut job_fut).await.ok(),
            }
        };

        self.clear_work().await;

        let Some(result) = outcome else {
            // Interrupted by shutdown after the grace window: requeue the job
            // (and, in reliable mode, drop its in-progress copy so recovery
            // doesn't run it a second time) so the next process picks it up.
            self.requeue_interrupted(&work).await;
            return Ok(WorkFetcher::Done);
        };

        // Reliable fetch: the job has reached a terminal state inside
        // `chain.call` — it either succeeded or the retry middleware already
        // moved it to the `retry`/`dead` set — so drop our in-progress copy.
        // Done regardless of `result`: a failed job already lives in
        // `retry`/`dead`, and leaving the in-progress copy would let orphan
        // recovery re-run it later as a duplicate.
        if let Some(claim) = &work.reliable {
            self.reliable_ack(claim).await;
        }

        result?;

        // TODO: Make this only say "done" when the job is successful.
        // We might need to change the ChainIter to return the final job and
        // detect any retries?
        info!({
            "elapsed" = format!("{:?}", started.elapsed()),
            "status" = "done",
            "class" = &work.job.class,
            "queue" = &work.job.queue,
            "jid" = &work.job.jid}, "sidekiq");

        Ok(WorkFetcher::Done)
    }

    /// Push a job that was interrupted by shutdown back onto its queue so a
    /// surviving/replacement process runs it — the free-Sidekiq
    /// `BasicFetch#bulk_requeue` behavior ("worse to lose a job than to run it
    /// twice", i.e. at-least-once).
    ///
    /// In reliable mode this is an **atomic** move (`requeue_if_inprogress`):
    /// the job is RPUSH'd back *only if* it's still in our in-progress list. If
    /// orphan recovery on the replacement process already requeued it (it
    /// drains dead processes' in-progress lists, and our heartbeat is gone by
    /// the time we get here), the move is a no-op — so the shutdown requeue and
    /// recovery never both requeue the same job and produce a duplicate. In
    /// BRPOP mode the job lives only in memory (no in-progress copy, no
    /// recovery to race), so it's pushed back unconditionally. Best-effort: a
    /// Redis error is logged; in reliable mode orphan recovery is the backstop.
    async fn requeue_interrupted(&self, work: &UnitOfWork) {
        let queue_key = format!("queue:{}", work.job.queue);

        let result: Result<bool> = async {
            let mut conn = self.redis.get().await?;
            match &work.reliable {
                Some(claim) => Ok(conn
                    .requeue_if_inprogress(
                        claim.inprogress_key.clone(),
                        queue_key.clone(),
                        claim.job_raw.clone(),
                    )
                    .await?),
                None => {
                    conn.rpush(queue_key.clone(), serde_json::to_string(&work.job)?)
                        .await?;
                    Ok(true)
                }
            }
        }
        .await;

        match result {
            Ok(true) => info!(
                target: "sidekiq",
                class = %work.job.class,
                jid = %work.job.jid,
                queue = %work.job.queue,
                "requeued in-flight job interrupted by shutdown",
            ),
            Ok(false) => info!(
                target: "sidekiq",
                class = %work.job.class,
                jid = %work.job.jid,
                "shutdown: job already requeued by recovery — not duplicating",
            ),
            Err(e) => error!(
                jid = %work.job.jid,
                "requeue on shutdown failed (reliable-fetch recovery is the backstop): {:?}",
                e
            ),
        }
    }

    /// Record an in-flight job in this process's Sidekiq WorkSet
    /// (`<identity>:work`) so it shows on the web UI "Busy" page. Best-effort:
    /// any Redis error is logged and never interrupts job processing. A no-op
    /// unless an `identity` + `tid` were assigned (i.e. running under `run()`).
    async fn set_work(&self, work: &UnitOfWork) {
        let (Some(identity), Some(tid)) = (self.identity.as_deref(), self.tid.as_deref()) else {
            return;
        };

        let result: Result<()> = async {
            let key = format!("{identity}:work");
            let mut conn = self.redis.get().await?;
            conn.hset(key.clone(), tid.to_string(), work_record(&work.job)?)
                .await?;
            conn.expire(key, 60).await?;
            Ok(())
        }
        .await;

        if let Err(err) = result {
            error!("Error recording sidekiq work state: {:?}", err);
        }
    }

    /// Clear this worker's WorkSet entry once the job finishes (success or fail).
    async fn clear_work(&self) {
        let (Some(identity), Some(tid)) = (self.identity.as_deref(), self.tid.as_deref()) else {
            return;
        };

        let result: Result<()> = async {
            let mut conn = self.redis.get().await?;
            conn.hdel(format!("{identity}:work"), tid.to_string())
                .await?;
            Ok(())
        }
        .await;

        if let Err(err) = result {
            error!("Error clearing sidekiq work state: {:?}", err);
        }
    }

    /// Reliable fetch: remove a finished job from its in-progress list. The
    /// payload carries a unique `jid`, so `LREM` with `count = -1` (one match
    /// scanning from the tail) removes exactly this job's copy. Best-effort: a
    /// failure only means the job lingers in the list and may be re-run by
    /// orphan recovery once this process's heartbeat expires (at-least-once).
    async fn reliable_ack(&self, claim: &ReliableClaim) {
        let result: Result<()> = async {
            let mut conn = self.redis.get().await?;
            let _: usize = conn
                .lrem(claim.inprogress_key.clone(), -1, claim.job_raw.clone())
                .await?;
            Ok(())
        }
        .await;

        if let Err(err) = result {
            error!(
                inprogress = %claim.inprogress_key,
                "reliable fetch: failed to ack (LREM) finished job: {:?}",
                err
            );
        }
    }

    pub fn register<
        Args: Sync + Send + for<'de> serde::Deserialize<'de> + 'static,
        W: Worker<Args> + 'static,
    >(
        &mut self,
        worker: W,
    ) {
        self.workers
            .insert(W::class_name(), Arc::new(WorkerRef::wrap(Arc::new(worker))));
    }

    pub fn get_cancellation_token(&self) -> CancellationToken {
        self.cancellation_token.clone()
    }

    pub(crate) async fn register_periodic(&mut self, periodic_job: PeriodicJob) -> Result<()> {
        self.periodic_jobs.push(periodic_job.clone());

        let mut conn = self.redis.get().await?;
        periodic_job.insert(&mut conn).await?;

        info!({
            "args" = &periodic_job.args,
            "class" = &periodic_job.class,
            "queue" = &periodic_job.queue,
            "name" = &periodic_job.name,
            "cron" = &periodic_job.cron,
        },"Inserting periodic job");

        Ok(())
    }

    /// Takes self to consume the processor. This is for life-cycle management, not
    /// memory safety because you can clone processor pretty easily.
    pub async fn run(self) {
        let mut join_set: JoinSet<()> = JoinSet::new();

        // Build the stats publisher up front so its process identity can be shared
        // with the workers: each worker records its in-flight job under that
        // identity's WorkSet (`<identity>:work`) — the same identity the heartbeat
        // registers in the `processes` set — so the web "Busy" page lists running
        // jobs against this process.
        let hostname = if let Some(host) = gethostname::gethostname().to_str() {
            host.to_string()
        } else {
            "UNKNOWN_HOSTNAME".to_string()
        };
        let stats_publisher = StatsPublisher::new(
            hostname,
            self.human_readable_queues.clone(),
            self.busy_jobs.clone(),
            self.config.num_workers,
        );
        let identity = stats_publisher.identity().to_string();

        // Publish one heartbeat synchronously BEFORE any worker can fetch, so a
        // sibling process running reliable-fetch orphan recovery never mistakes
        // this freshly-booted process for a dead one (its heartbeat hash exists
        // from t=0, closing the window before the 5s stats loop's first beat).
        if let Err(err) = stats_publisher.publish_stats(self.redis.clone()).await {
            error!("Error publishing initial processor heartbeat: {:?}", err);
        }

        // Reliable fetch: requeue jobs stranded in the in-progress lists of
        // processes that died before us (the deploy/restart case). The periodic
        // sweep spawned below catches deaths that happen while we're running.
        if self.config.reliable {
            match recover_orphaned_work(&self.redis, &identity, WORKING_SET).await {
                Ok(n) if n > 0 => {
                    info!(
                        recovered = n,
                        "reliable fetch: requeued orphaned jobs at boot"
                    )
                }
                Ok(_) => {}
                Err(err) => error!(
                    "reliable fetch: boot-time orphan recovery failed: {:?}",
                    err
                ),
            }
        }

        // Logic for spawning shared workers (workers that handles multiple queues) and dedicated
        // workers (workers that handle a single queue).
        let spawn_worker = |mut processor: Processor,
                            cancellation_token: CancellationToken,
                            num: usize,
                            dedicated_queue_name: Option<String>| {
            async move {
                loop {
                    if let Err(err) = processor.process_one().await {
                        error!("Error leaked out the bottom: {:?}", err);
                    }

                    if cancellation_token.is_cancelled() {
                        break;
                    }
                }

                let dedicated_queue_str = dedicated_queue_name
                    .map(|name| format!(" dedicated to queue '{name}'"))
                    .unwrap_or_default();
                debug!("Broke out of loop for worker {num}{dedicated_queue_str}");
            }
        };

        // Start worker routines.
        for i in 0..self.config.num_workers {
            let mut processor = self.clone();
            processor.identity = Some(identity.clone());
            processor.tid = Some(generate_tid());
            join_set.spawn(spawn_worker(
                processor,
                self.cancellation_token.clone(),
                i,
                None,
            ));
        }

        // Start dedicated worker routines.
        for (queue, config) in &self.config.queue_configs {
            for i in 0..config.num_workers {
                join_set.spawn({
                    let mut processor = self.clone();
                    processor.queues = [queue.clone()].into();
                    processor.identity = Some(identity.clone());
                    processor.tid = Some(generate_tid());
                    spawn_worker(
                        processor,
                        self.cancellation_token.clone(),
                        i,
                        Some(queue.clone()),
                    )
                });
            }
        }

        // Start sidekiq-web metrics publisher. Consumes the `stats_publisher` built
        // above (whose identity the workers share for the WorkSet).
        join_set.spawn({
            let redis = self.redis.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = stats_publisher.publish_stats(redis.clone()).await {
                        error!("Error publishing processor stats: {:?}", err);
                    }
                }

                // On graceful shutdown, remove the process from the `processes` set and
                // delete the heartbeat hash. This mirrors Ruby Sidekiq's clear_heartbeat():
                //   pipeline.srem("processes", [identity])
                //   pipeline.unlink("#{identity}:work")
                // Without this, stale entries accumulate in the `processes` set until the
                // heartbeat hash's 60-second TTL expires — but the set membership has no TTL
                // and never self-cleans.
                let identity = stats_publisher.identity().to_string();
                if let Err(err) = stats_publisher.deregister(redis.clone()).await {
                    error!(
                        identity = %identity,
                        "Error deregistering processor from Redis on shutdown: {:?}",
                        err
                    );
                }

                debug!(identity = %identity, "Deregistered processor from Redis");
            }
        });

        // Start retry and scheduled routines.
        join_set.spawn({
            let redis = self.redis.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let sched = Scheduled::new(redis);
                let sorted_sets = vec!["retry".to_string(), "schedule".to_string()];

                loop {
                    // TODO: Use process count to meet a 5 second avg.
                    select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(5)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = sched.enqueue_jobs(chrono::Utc::now(), &sorted_sets).await {
                        error!("Error in scheduled poller routine: {:?}", err);
                    }
                }

                debug!("Broke out of loop for retry and scheduled");
            }
        });

        // Watch for periodic jobs and enqueue jobs.
        join_set.spawn({
            let redis = self.redis.clone();
            let cancellation_token = self.cancellation_token.clone();
            async move {
                let sched = Scheduled::new(redis);

                loop {
                    // TODO: Use process count to meet a 30 second avg.
                    select! {
                        _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                        _ = cancellation_token.cancelled() => {
                            break;
                        }
                    }

                    if let Err(err) = sched.enqueue_periodic_jobs(chrono::Utc::now()).await {
                        error!("Error in periodic job poller routine: {}", err);
                    }
                }

                debug!("Broke out of loop for periodic");
            }
        });

        // Reliable fetch: periodically requeue jobs stranded by processes that
        // died while we were running. Liveness is the heartbeat hash's 60s TTL,
        // so a killed process's jobs are recovered within ~60s.
        if self.config.reliable {
            join_set.spawn({
                let redis = self.redis.clone();
                let cancellation_token = self.cancellation_token.clone();
                let identity = identity.clone();
                async move {
                    loop {
                        select! {
                            _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {}
                            _ = cancellation_token.cancelled() => {
                                break;
                            }
                        }

                        match recover_orphaned_work(&redis, &identity, WORKING_SET).await {
                            Ok(n) if n > 0 => {
                                info!(recovered = n, "reliable fetch: requeued orphaned jobs")
                            }
                            Ok(_) => {}
                            Err(err) => {
                                error!("reliable fetch: orphan recovery sweep failed: {:?}", err)
                            }
                        }
                    }

                    debug!("Broke out of loop for reliable-fetch orphan recovery");
                }
            });
        }

        while let Some(result) = join_set.join_next().await {
            if let Err(err) = result {
                error!("Processor had a spawned task return an error: {}", err);
            }
        }
    }

    pub async fn using<M>(&mut self, middleware: M)
    where
        M: ServerMiddleware + Send + Sync + 'static,
    {
        self.chain.using(Box::new(middleware)).await;
    }
}

/// Build the value stored in the `<identity>:work` hash for an in-flight job,
/// matching Ruby Sidekiq's `{queue, payload, run_at}` work record. `payload` is
/// the job JSON as a *string*, exactly as Sidekiq stores it and the web UI
/// expects it (`Sidekiq.load_json(work.payload)`).
fn work_record(job: &Job) -> Result<String> {
    let record = serde_json::json!({
        "queue": job.queue,
        "payload": serde_json::to_string(job)?,
        "run_at": chrono::Utc::now().timestamp(),
    });

    Ok(record.to_string())
}

/// Requeue jobs stranded in the in-progress lists of dead processes.
///
/// Reliable fetch moves each claimed job into a per-process list
/// `queue:<q>:inprogress:<identity>` and records that list in [`WORKING_SET`].
/// A process that dies mid-job leaves its claimed jobs there. This walks the
/// registry and, for every list whose owning process is no longer alive,
/// moves the jobs back onto their original queue and drops the registry entry.
/// Returns the number of jobs requeued.
///
/// Liveness is the heartbeat hash (`EXISTS <identity>`, a 60s TTL refreshed
/// every 5s by the stats loop) — **not** membership in the `processes` set,
/// which has no TTL and lingers after an ungraceful death. Concurrent
/// recoverers are safe: each job is moved by exactly one `RPOPLPUSH` (any
/// other recoverer sees an empty list), and `SREM` is idempotent.
pub(crate) async fn recover_orphaned_work(
    redis: &RedisPool,
    my_identity: &str,
    working_set: &str,
) -> Result<usize> {
    let mut conn = redis.get().await?;
    let members: Vec<String> = conn.smembers(working_set.to_string()).await?;
    let mut requeued = 0usize;

    for member in members {
        // member == "queue:<q>:inprogress:<identity>". Split on the last
        // ":inprogress:" so the queue key (which may contain ':') and the owner
        // identity (host:pid:nonce, also ':'-laden) are both recovered intact.
        let Some((queue_key, owner)) = member.rsplit_once(":inprogress:") else {
            // Unrecognized entry — drop it so the registry can't grow unbounded.
            let _ = conn.srem(working_set.to_string(), member.clone()).await;
            continue;
        };

        // Our own list — we're alive, leave it.
        if owner == my_identity {
            continue;
        }

        // Owner still alive (heartbeat present) — leave its in-flight work.
        if conn.exists(owner.to_string()).await? {
            continue;
        }

        // Owner is dead: move every stranded job back onto its queue.
        while conn
            .rpoplpush(member.clone(), queue_key.to_string())
            .await?
            .is_some()
        {
            requeued += 1;
        }

        // The list is now empty; drop the registry entry. If the owner somehow
        // revives it will re-register on its next claim.
        let _ = conn.srem(working_set.to_string(), member.clone()).await;
    }

    Ok(requeued)
}

#[cfg(test)]
mod work_set_tests {
    use super::*;

    #[test]
    fn work_record_matches_sidekiq_shape() {
        let job: Job = serde_json::from_str(
            r#"{"queue":"default","args":[1,"x"],"retry":true,"class":"HardWorker","jid":"abc123","created_at":1700000000.0}"#,
        )
        .expect("parse job");

        let record: serde_json::Value =
            serde_json::from_str(&work_record(&job).expect("build record")).expect("parse record");

        assert_eq!(record["queue"], "default");
        assert!(record["run_at"].is_number());

        // `payload` must be a JSON *string* (the job JSON), not a nested object.
        let payload = record["payload"].as_str().expect("payload is a string");
        let payload: serde_json::Value = serde_json::from_str(payload).expect("parse payload");
        assert_eq!(payload["class"], "HardWorker");
        assert_eq!(payload["jid"], "abc123");
        assert_eq!(payload["args"][1], "x");
        assert!(payload["args"][0].is_number());
    }
}

/// Reliable-fetch + orphan-recovery integration tests. These require a Redis
/// listening on `redis://127.0.0.1/` (same as the other tests in this crate).
/// Every test uses uniquely-named queues / identities / working sets so they
/// stay isolated when the suite runs concurrently against one Redis.
#[cfg(test)]
mod reliable_fetch_tests {
    use super::*;
    use crate::{ProcessorConfig, RedisConnectionManager, RedisPool};
    use bb8::Pool;
    // Tests that touch the shared real `WORKING_SET` are `#[serial]` so one
    // test's recovery sweep can't drain another's freshly-claimed (heartbeat-
    // less) in-progress list mid-run. The recover-only tests above use unique
    // working-set keys and need no serialization.
    use serial_test::serial;

    async fn test_pool() -> RedisPool {
        let manager = RedisConnectionManager::new("redis://127.0.0.1/").unwrap();
        Pool::builder().build(manager).await.unwrap()
    }

    /// Unique suffix so concurrent tests never share keys.
    fn unique(prefix: &str) -> String {
        format!("{prefix}_{}", generate_tid())
    }

    async fn lpush(redis: &RedisPool, key: &str, val: &str) {
        let mut conn = redis.get().await.unwrap();
        let _: i64 = redis::cmd("LPUSH")
            .arg(key)
            .arg(val)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    async fn llen(redis: &RedisPool, key: &str) -> usize {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("LLEN")
            .arg(key)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap()
    }

    async fn sadd(redis: &RedisPool, set: &str, member: &str) {
        let mut conn = redis.get().await.unwrap();
        let _: i64 = redis::cmd("SADD")
            .arg(set)
            .arg(member)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    async fn sismember(redis: &RedisPool, set: &str, member: &str) -> bool {
        let mut conn = redis.get().await.unwrap();
        redis::cmd("SISMEMBER")
            .arg(set)
            .arg(member)
            .query_async::<i64>(conn.unnamespaced_borrow_mut())
            .await
            .unwrap()
            == 1
    }

    /// Mark an identity "alive" by creating a key named exactly like its
    /// heartbeat hash (recovery checks `EXISTS <identity>`).
    async fn mark_alive(redis: &RedisPool, identity: &str) {
        let mut conn = redis.get().await.unwrap();
        let _: () = redis::cmd("SET")
            .arg(identity)
            .arg("1")
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    async fn del(redis: &RedisPool, key: &str) {
        let mut conn = redis.get().await.unwrap();
        let _: i64 = redis::cmd("DEL")
            .arg(key)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn recover_requeues_dead_owners_jobs() {
        let redis = test_pool().await;
        let queue_key = format!("queue:{}", unique("rf_dead"));
        let dead_owner = unique("deadhost:111");
        let member = format!("{queue_key}:inprogress:{dead_owner}");
        let working_set = unique("rf_working");

        // Two jobs left stranded in the dead process's in-progress list.
        lpush(&redis, &member, "job-a").await;
        lpush(&redis, &member, "job-b").await;
        sadd(&redis, &working_set, &member).await;

        let n = recover_orphaned_work(&redis, &unique("livehost:222"), &working_set)
            .await
            .unwrap();

        assert_eq!(n, 2, "both stranded jobs should be requeued");
        assert_eq!(llen(&redis, &member).await, 0, "in-progress list drained");
        assert_eq!(
            llen(&redis, &queue_key).await,
            2,
            "jobs moved back to the queue"
        );
        assert!(
            !sismember(&redis, &working_set, &member).await,
            "registry entry removed once the dead list is drained"
        );

        del(&redis, &queue_key).await;
        del(&redis, &working_set).await;
    }

    #[tokio::test]
    async fn recover_skips_alive_owner() {
        let redis = test_pool().await;
        let queue_key = format!("queue:{}", unique("rf_alive"));
        let owner = unique("alivehost:111");
        let member = format!("{queue_key}:inprogress:{owner}");
        let working_set = unique("rf_working");

        lpush(&redis, &member, "job-a").await;
        sadd(&redis, &working_set, &member).await;
        mark_alive(&redis, &owner).await; // heartbeat present ⇒ alive

        let n = recover_orphaned_work(&redis, &unique("livehost:222"), &working_set)
            .await
            .unwrap();

        assert_eq!(n, 0, "an alive owner's in-flight job must not be touched");
        assert_eq!(
            llen(&redis, &member).await,
            1,
            "in-progress list left intact"
        );
        assert!(
            sismember(&redis, &working_set, &member).await,
            "registry entry kept"
        );

        del(&redis, &member).await;
        del(&redis, &working_set).await;
        del(&redis, &owner).await;
    }

    #[tokio::test]
    async fn recover_skips_our_own_inprogress_list() {
        let redis = test_pool().await;
        let queue_key = format!("queue:{}", unique("rf_self"));
        let me = unique("selfhost:111");
        let member = format!("{queue_key}:inprogress:{me}");
        let working_set = unique("rf_working");

        lpush(&redis, &member, "job-a").await;
        sadd(&redis, &working_set, &member).await;

        // We have no heartbeat key here, proving the skip is by identity match,
        // not by liveness — a process never reclaims its own in-flight work.
        let n = recover_orphaned_work(&redis, &me, &working_set)
            .await
            .unwrap();

        assert_eq!(n, 0, "our own list is never reclaimed");
        assert_eq!(llen(&redis, &member).await, 1);
        assert!(sismember(&redis, &working_set, &member).await);

        del(&redis, &member).await;
        del(&redis, &working_set).await;
    }

    #[tokio::test]
    #[serial]
    async fn reliable_fetch_claims_into_inprogress_then_acks() {
        let redis = test_pool().await;
        let q = unique("rf_claim");
        let queue_key = format!("queue:{q}");
        let identity = unique("claimhost:111");
        let inprogress = format!("{queue_key}:inprogress:{identity}");
        let jid = unique("jid");
        let payload = format!(
            r#"{{"queue":"{q}","args":[],"retry":true,"class":"HardWorker","jid":"{jid}","created_at":1700000000.0}}"#
        );

        lpush(&redis, &queue_key, &payload).await;

        let mut processor = Processor::new(redis.clone(), vec![q.clone()])
            .with_config(ProcessorConfig::default().num_workers(1).reliable(true));
        processor.identity = Some(identity.clone());

        let uow = processor
            .fetch_reliable(&identity)
            .await
            .unwrap()
            .expect("a job should be claimed");

        // The claim moved the job out of the queue and into our in-progress
        // list, and tagged the unit of work with the ack bookkeeping.
        assert_eq!(uow.job.jid, jid);
        let claim = uow.reliable.as_ref().expect("claim is reliable");
        assert_eq!(claim.inprogress_key, inprogress);
        assert_eq!(llen(&redis, &queue_key).await, 0, "job removed from queue");
        assert_eq!(
            llen(&redis, &inprogress).await,
            1,
            "job parked in in-progress list"
        );
        assert!(
            sismember(&redis, WORKING_SET, &inprogress).await,
            "in-progress list registered for orphan recovery"
        );

        // Acking (the path taken when the job finishes) clears the copy so it
        // can't be re-run by recovery.
        processor.reliable_ack(claim).await;
        assert_eq!(
            llen(&redis, &inprogress).await,
            0,
            "ack removed the in-progress copy"
        );

        // Leave the shared WORKING_SET as we found it.
        let mut conn = redis.get().await.unwrap();
        let _: i64 = redis::cmd("SREM")
            .arg(WORKING_SET)
            .arg(&inprogress)
            .query_async(conn.unnamespaced_borrow_mut())
            .await
            .unwrap();
    }

    /// End-to-end: a worker claims a job via the real `fetch_reliable` path,
    /// then "crashes" (we never ack and it has no heartbeat), and a sibling's
    /// `recover_orphaned_work` sweep returns the stranded job to its queue.
    /// Asserts on the specific queue/list rather than the requeued count so it
    /// tolerates unrelated entries in the shared `WORKING_SET`.
    #[tokio::test]
    #[serial]
    async fn reliable_fetch_then_recovery_returns_stranded_job_to_queue() {
        let redis = test_pool().await;
        let q = unique("rf_e2e");
        let queue_key = format!("queue:{q}");
        let dead_identity = unique("e2ehost:111"); // never gets a heartbeat
        let inprogress = format!("{queue_key}:inprogress:{dead_identity}");
        let jid = unique("jid");
        let payload = format!(
            r#"{{"queue":"{q}","args":[],"retry":true,"class":"HardWorker","jid":"{jid}","created_at":1700000000.0}}"#
        );

        lpush(&redis, &queue_key, &payload).await;

        // Worker claims the job through the real fetch path, then crashes
        // (function returns, we never ack — simulating a killed process).
        let mut processor = Processor::new(redis.clone(), vec![q.clone()])
            .with_config(ProcessorConfig::default().num_workers(1).reliable(true));
        processor.identity = Some(dead_identity.clone());
        let uow = processor
            .fetch_reliable(&dead_identity)
            .await
            .unwrap()
            .expect("a job should be claimed");
        assert_eq!(uow.job.jid, jid);
        assert_eq!(
            llen(&redis, &queue_key).await,
            0,
            "claimed out of the queue"
        );
        assert_eq!(
            llen(&redis, &inprogress).await,
            1,
            "parked in the in-progress list"
        );
        drop(uow); // the "crash": the claim is dropped without ever being acked

        // A sibling process sweeps: the crashed worker has no heartbeat, so its
        // stranded job is requeued.
        recover_orphaned_work(&redis, &unique("siblinghost:222"), WORKING_SET)
            .await
            .unwrap();

        assert_eq!(
            llen(&redis, &inprogress).await,
            0,
            "in-progress list drained by recovery"
        );
        assert_eq!(
            llen(&redis, &queue_key).await,
            1,
            "stranded job returned to its queue"
        );
        assert!(
            !sismember(&redis, WORKING_SET, &inprogress).await,
            "registry entry cleared once drained"
        );

        del(&redis, &queue_key).await;
    }

    /// The shutdown requeue (`BasicFetch#bulk_requeue` parity): a job
    /// interrupted by SIGTERM is pushed back onto its queue, and in reliable
    /// mode its in-progress copy is removed so recovery won't double it.
    #[tokio::test]
    async fn requeue_interrupted_pushes_back_and_clears_inprogress() {
        let redis = test_pool().await;
        let q = unique("rf_intr");
        let queue_key = format!("queue:{q}");
        let identity = unique("intrhost:1");
        let inprogress = format!("{queue_key}:inprogress:{identity}");
        let jid = unique("jid");
        let payload = format!(
            r#"{{"queue":"{q}","args":[],"retry":true,"class":"HardWorker","jid":"{jid}","created_at":1700000000.0}}"#
        );

        // Simulate a reliably-claimed job: its payload sits in the in-progress
        // list and the main queue is empty.
        lpush(&redis, &inprogress, &payload).await;

        let processor = Processor::new(redis.clone(), vec![q.clone()]);
        let job: crate::Job = serde_json::from_str(&payload).unwrap();
        let work = crate::UnitOfWork {
            queue: queue_key.clone(),
            job,
            reliable: Some(crate::ReliableClaim {
                inprogress_key: inprogress.clone(),
                job_raw: payload.clone(),
            }),
        };

        processor.requeue_interrupted(&work).await;

        assert_eq!(
            llen(&redis, &queue_key).await,
            1,
            "job pushed back onto its queue"
        );
        assert_eq!(
            llen(&redis, &inprogress).await,
            0,
            "in-progress copy removed"
        );

        del(&redis, &queue_key).await;
    }

    /// If orphan recovery already requeued the job (its in-progress copy is
    /// gone), the shutdown requeue is a no-op — it must NOT push a duplicate.
    /// This is the race that produced two copies of the same backfill jid.
    #[tokio::test]
    async fn requeue_interrupted_does_not_duplicate_already_recovered_job() {
        let redis = test_pool().await;
        let q = unique("rf_intr_dup");
        let queue_key = format!("queue:{q}");
        let identity = unique("duphost:1");
        let inprogress = format!("{queue_key}:inprogress:{identity}");
        let jid = unique("jid");
        let payload = format!(
            r#"{{"queue":"{q}","args":[],"retry":true,"class":"HardWorker","jid":"{jid}","created_at":1700000000.0}}"#
        );

        // In-progress list is EMPTY — i.e. recovery already moved the job out.
        let processor = Processor::new(redis.clone(), vec![q.clone()]);
        let job: crate::Job = serde_json::from_str(&payload).unwrap();
        let work = crate::UnitOfWork {
            queue: queue_key.clone(),
            job,
            reliable: Some(crate::ReliableClaim {
                inprogress_key: inprogress.clone(),
                job_raw: payload.clone(),
            }),
        };

        processor.requeue_interrupted(&work).await;

        assert_eq!(
            llen(&redis, &queue_key).await,
            0,
            "must NOT requeue a job recovery already took (no duplicate)"
        );

        del(&redis, &queue_key).await;
    }

    /// Non-reliable (BRPOP) jobs are also requeued on shutdown — the payload is
    /// re-serialized from the job (there's no in-progress copy to move).
    #[tokio::test]
    async fn requeue_interrupted_brpop_job_is_pushed_back() {
        let redis = test_pool().await;
        let q = unique("rf_intr_basic");
        let queue_key = format!("queue:{q}");
        let jid = unique("jid");
        let payload = format!(
            r#"{{"queue":"{q}","args":[],"retry":true,"class":"HardWorker","jid":"{jid}","created_at":1700000000.0}}"#
        );

        let processor = Processor::new(redis.clone(), vec![q.clone()]);
        let job: crate::Job = serde_json::from_str(&payload).unwrap();
        let work = crate::UnitOfWork {
            queue: queue_key.clone(),
            job,
            reliable: None,
        };

        processor.requeue_interrupted(&work).await;

        assert_eq!(
            llen(&redis, &queue_key).await,
            1,
            "BRPOP job pushed back onto its queue"
        );

        del(&redis, &queue_key).await;
    }
}
