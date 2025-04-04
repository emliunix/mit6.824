use std::{cmp::Reverse, collections::{BTreeMap, BinaryHeap, VecDeque}, pin::Pin, sync::Arc};

use anyhow::anyhow;
use futures::{future::Shared, FutureExt};
use tokio::{net::UnixListener, sync::{mpsc, oneshot, Mutex, Semaphore}};
use tokio_stream::{wrappers::UnixListenerStream, Stream};
use tonic::{async_trait, transport::Server, Request, Response, Status, Streaming};

use super::rpc::{master_sock, proto};

#[derive(Debug)]
struct Task {
    id: i64,
    task: proto::Task,
    oneshot: oneshot::Sender<proto::task_result::TaskResultType>,
}

struct WorkerConnection {
    id: i64,
    task_sender: mpsc::UnboundedSender<proto::Task>,
    tasks: BTreeMap<i64, Task>,
}

struct MasterInner {
    client_seq: i64,
    task_seq: i64,
    task_queue: VecDeque<Task>,
    workers: Vec<WorkerConnection>,
    workers_heap: BinaryHeap<Reverse<(usize, usize)>>, // (remaining_tasks, worker_index)
}

#[derive(Clone)]
struct MasterServer {
    inner: Arc<Mutex<MasterInner>>,
    shutdown_receiver: Shared<Pin<Box<dyn Future<Output=()> + Send>>>,
}

impl WorkerConnection {
    fn new(id: i64, task_sender: mpsc::UnboundedSender<proto::Task>) -> WorkerConnection {
        WorkerConnection {
            id, task_sender,
            tasks: BTreeMap::new(),
        }
    }
}

#[async_trait]
impl proto::master_server::Master for MasterServer {
    async fn gen_client_id(&self, _request: Request<()>) -> Result<Response<proto::ClientId>, Status> {
        let id = self.new_client_id().await;
        Ok(Response::new(proto::ClientId { id }))
    }

    type getTaskStream = Pin<Box<dyn Stream<Item=Result<proto::Task, Status>> + Send + 'static>>;

    async fn get_task(&self, request: Request<proto::GetTaskRequest>) -> Result<Response<Self::getTaskStream>, Status> {
        let request = request.into_inner();
        let id = request.client_id.unwrap().id;
        log::info!("Worker#{} connected", id);
        let self_ = self.clone();
        Ok(Response::new(Box::pin(async_stream::try_stream! {
            let (task_sender, mut receiver)= mpsc::unbounded_channel::<proto::Task>();
            self_.add_client(WorkerConnection::new(id, task_sender)).await;
            loop {
                if let Some(task) = receiver.recv().await {
                    yield task;
                } else {
                    log::info!("Worker#{} disconnected", id);
                break;
                }
            }
        })))
    }

    async fn finish_task(&self, request: Request<proto::FinishTaskRequest>) -> Result<Response<()>, Status> {
        let task = request.into_inner();
        let client_id = task.client_id.unwrap().id;
        let task_id = task.task_id;
        log::debug!("Worker#{} finished task#{}, is_error: {}, error_msg: {}",
                   client_id, task_id,
                   task.is_error, task.error_msg.unwrap_or_else(|| "".to_string()));
        if task.is_error {
            self.fail_task(client_id, task_id).await;
        } else {
            self.finish_task(client_id, task_id, task.result.unwrap()).await;
        }
        Ok(Response::new(()))
    }

    async fn heartbeat(&self, request: Request<Streaming<proto::Heartbeat>>) -> Result<Response<()>, Status> {
        let mut hbs = request.into_inner();
        let mut client_id = None;
        let fut_heartbeats = async {
            loop {
                match hbs.message().await {
                    Ok(Some(hb)) => {
                        client_id = hb.client_id;
                        match &client_id {
                            Some(id) => log::trace!("Heartbeat from worker#{}", id.id),
                            None => log::warn!("Invalid heartbeat"),
                        };
                    }
                    Ok(None) => break,
                    Err(e) => {
                        log::error!("Error receiving heartbeat: {:?}", e);
                        break;
                    }
                }
            }
            // client exits
            if let Some(client_id) = client_id {
                log::info!("Worker#{} exited", client_id.id);
                self.evict_client(client_id.id).await;
            }
        };

        tokio::select! {
            _ = self.shutdown_receiver.clone() => (),
            _ = fut_heartbeats => (),
        };
        Ok(Response::new(()))
    }
}

impl MasterServer {
    pub fn new<F>(shutdown_receiver: F) -> Self
    where
        F: Future<Output=()> + Send + 'static,
    {
        let shutdown_receiver = (
            Box::pin(shutdown_receiver) as
                Pin<Box<dyn Future<Output=()> + Send>>
        ).shared();
        let inner = Arc::new(Mutex::new(MasterInner {
            client_seq: 0,
            task_seq: 0,
            task_queue: VecDeque::new(),
            workers: Vec::new(),
            workers_heap: BinaryHeap::new(),
        }));
        Self { inner, shutdown_receiver, }
    }
    
    async fn run(&self) -> Result<(), anyhow::Error> {
        log::info!("Master starting");
        
        // delete master socket if exists
        let sock = master_sock();
        let _ = std::fs::remove_file(&sock);
        Server::builder()
            .add_service(proto::master_server::MasterServer::new(self.clone()))
            .serve_with_incoming_shutdown(
                UnixListenerStream::new(UnixListener::bind(&sock)?),
                self.shutdown_receiver.clone(),
            )
            .await?;

        log::info!("Master exiting");
        Ok(())
    }

    /* -- called by main task -- */

    async fn schedule_task(&self, task: proto::task::TaskType) -> Result<proto::task_result::TaskResultType, anyhow::Error> {
        let (oneshot, receiver) = oneshot::channel();
        let id = self.new_task_id().await;
        let task = Task {
            id,
            task: proto::Task {
                id,
                task_type: Some(task),
            },
            oneshot,
        };
        self.enqueue_task(task).await;
        self.try_dispatch_task().await;

        let res = receiver.await?;
        Ok(res)
    }

    async fn shutdown(&self) {
        let mut inner = self.inner.lock().await;
        inner.clear_clients();
    }

    /* -- called by worker -- */
    
    async fn new_client_id(&self) -> i64 {
        let mut inner = self.inner.lock().await;
        inner.new_client_id()
    }

    async fn add_client(&self, client: WorkerConnection) {
        {
            let mut inner = self.inner.lock().await;
            inner.add_client(client);
        }
        // in case tasks pending
        self.try_dispatch_task().await;
    }

    async fn evict_client(&self, client_id: i64) {
        let had_worker = {
            let mut inner = self.inner.lock().await;
            inner.evict_client_and_requeue(client_id)
        };
        if had_worker {
            self.try_dispatch_task().await;
        }
    }

    async fn fail_task(&self, worker: i64, task: i64) {
        let mut inner = self.inner.lock().await;
        if let Some(task) = inner.remove_task(worker, task) {
            log::info!("Requeueing failed task#{}: {:?}", task.id, task.task);
            self.enqueue_task(task).await;
        }
    }

    async fn finish_task(&self, worker: i64, task: i64, result: proto::TaskResult) {
        let mut inner = self.inner.lock().await;
        if let Some(task) = inner.remove_task(worker, task) {
            log::debug!("Finishing task#{}: {:?}", task.id, task.task);
            // send result to worker
            let _ = task.oneshot.send(result.task_result_type.unwrap());
        }
    }

    /* -- dispatching -- */

    async fn try_dispatch_task(&self) {
        log::debug!("Dispatching tasks");
        let mut inner = self.inner.lock().await;
        loop {
            match inner.dispatch_task_1() {
                DispatchResult::HasMoreTasks => (),
                DispatchResult::Done => {
                    break;
                }
                DispatchResult::NoWorker => {
                    log::warn!("No available workers to dispatch task");
                    break;
                },
            };
        }
    }

    async fn new_task_id(&self) -> i64 {
        let mut inner = self.inner.lock().await;
        inner.new_task_id()
    }

    async fn enqueue_task(&self, task: Task) {
        let mut inner = self.inner.lock().await;
        inner.task_queue.push_back(task);
    }
}

enum DispatchResult {
    HasMoreTasks,
    NoWorker,
    Done,
}

impl MasterInner {
    fn new_task_id(&mut self) -> i64 {
        self.task_seq += 1;
        self.task_seq
    }

    fn new_client_id(&mut self) -> i64 {
        self.client_seq += 1;
        self.client_seq
    }

    fn add_client(&mut self, client: WorkerConnection) {
        self.workers.push(client);
        self.workers_heap.push(Reverse((0, self.workers.len() - 1)));
    }

    fn clear_clients(&mut self) {
        self.workers.clear();
        self.workers_heap.clear();
    }

    fn evict_client_and_requeue(&mut self, client_id: i64) -> bool {
        let i = if let Some((i, _)) = self.workers.iter().enumerate().find(|(_, w)| w.id == client_id) { i }
        else { log::warn!("worker#{} not found", client_id); return false; };
        let w = self.workers.remove(i);
        self.workers_heap.retain(|Reverse((_, j))| *j != i);
        log::info!("Evicting worker#{} and requeue {} tasks", client_id, w.tasks.len());
        self.task_queue.extend(w.tasks.into_values());
        true
    }

    fn dispatch_task_1(&mut self) -> DispatchResult {
        log::debug!("workers_heap: {:?}", self.workers_heap);
        if let Some(&Reverse((_, worker_index))) = self.workers_heap.peek() {
            if let Some(task) = self.task_queue.pop_front() {
                log::debug!("Dispatching task#{} to worker#{}", task.id, self.workers[worker_index].id);
                let worker = &mut self.workers[worker_index];
                if let Err(mpsc::error::SendError(_)) = worker.task_sender.send(task.task.clone()) {
                    let worker_id = worker.id;
                    log::warn!("worker#{} seems to be dead, evicting it", worker_id);
                    self.task_queue.push_front(task);
                    self.evict_client_and_requeue(worker_id);
                } else {
                    log::debug!("Task#{} sent to worker#{}", task.id, worker.id);
                    // task sent successfully, update heap to relfect n_processing change
                    worker.tasks.insert(task.id, task);
                    let ntasks = worker.tasks.len();
                    self.workers_heap.pop();
                    self.workers_heap.push(Reverse((ntasks, worker_index)));
                }
            }
        } else {
            return DispatchResult::NoWorker;
        }
        return if self.task_queue.is_empty() { 
            DispatchResult::Done
        } else {
            DispatchResult::HasMoreTasks
        };
    }

    fn remove_task(&mut self, worker: i64, task: i64) -> Option<Task> {
        let i = if let Some((i, _)) = self.workers.iter().enumerate().find(|(_, w)| w.id == worker) {
            i
        } else {
            log::warn!("Worker#{} not found", worker);
            return None;
        };
        let w = &mut self.workers[i];
        let task = if let Some(task) = w.tasks.remove(&task) {
            self.workers_heap.retain(|&Reverse((_, j))| j != i);
            self.workers_heap.push(Reverse((w.tasks.len(), i)));
            task
        } else {
            log::warn!("Task#{} not found in worker#{}", task, worker);
            return None;
        };
        return Some(task);
    }
}

pub struct Master;

impl Master {
    pub fn new() -> Self {
        Master
    }

    pub async fn run(&self, files: Vec<String>) -> Result<(), anyhow::Error> {
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        
        let shutdown_receiver = async move {
            shutdown_receiver.await.unwrap_or(())
        };

        let server = MasterServer::new(shutdown_receiver);

        let _ = tokio::try_join!(
            server.run(),
            master_task(shutdown_sender, server.clone(), files),
        );
        Ok(())
    }
}

async fn run_tasks<TI>(server: &MasterServer, tasks: TI) -> Result<Vec<proto::task_result::TaskResultType>, anyhow::Error>
where
    TI: IntoIterator<Item = proto::task::TaskType>,
{
    let res = Arc::new(Mutex::new(BTreeMap::new()));
    let sem = Arc::new(Semaphore::new(3));
    let mut handles = vec![];
    for (i, task) in tasks.into_iter().enumerate() {
        let perm = sem.clone().acquire_owned().await?;
        let res = res.clone();
        let server = server.clone();
        handles.push(tokio::spawn(async move {
            let task_res = server.schedule_task(task).await;
            let mut res = res.lock_owned().await;
            res.insert(i, task_res);
            drop(perm);
        }));
    }
    for h in handles { let _ = h.await; }

    let res = Arc::try_unwrap(res).unwrap().into_inner();
    let res_len = res.len();
    let mut out = Vec::with_capacity(res_len);
    for v in res.into_values() {
        let v = v?;
        out.push(v);
    }
    Ok(out)
}

const N_PARTITIONS: i64 = 8;

async fn master_task(shutdown_sender: oneshot::Sender<()>, server: MasterServer, inputs: Vec<String>) -> Result<(), anyhow::Error> {
    //     server.schedule_task(task);
    let map_tasks = inputs.into_iter().map(|file| {
        proto::task::TaskType::MapTask(proto::MapTask {
            app: "app".to_string(),
            n_partitions: N_PARTITIONS,
            input_file: file,
        })
    }).collect::<Vec<_>>();

    log::info!("running map tasks");
    let map_results = run_tasks(&server, map_tasks).await?;

    let mut reduce_files = BTreeMap::<i64, Vec<String>>::new();
    for map_res in map_results {
        let map_res = match map_res {
            proto::task_result::TaskResultType::MapTaskResult(res) => res,
            _ => return Err(anyhow!("illegal map result")),
        };
        for entry in map_res.outputs {
            reduce_files.entry(entry.partition)
                .and_modify(|files| files.push(entry.output_file.clone()))
                .or_insert(vec![entry.output_file.clone()]);
        }
    }
    let reduce_tasks = reduce_files.into_iter().map(|(part, files)| {
        proto::task::TaskType::ReduceTask(proto::ReduceTask {
            app: "app".to_string(),
            partition: part,
            input_files: files,
        })
    });
    let n_reduce_tasks = reduce_tasks.len();
    log::debug!("reduce_tasks: {:?}", reduce_tasks);
    log::info!("running reduce tasks");
    let reduce_results = run_tasks(&server, reduce_tasks).await?;

    let mut reduce_outputs = Vec::with_capacity(n_reduce_tasks);
    for res in reduce_results {
        match res {
            proto::task_result::TaskResultType::ReduceTaskResult(reduce_res) => reduce_outputs.extend(reduce_res.output_files.into_iter()),
            _ => return Err(anyhow!("illegal reduce result")),
        }
    }
    log::info!("result files: {:?}", reduce_outputs);
    log::info!("All tasks completed, shutting down");
    server.shutdown().await;
    let _ = shutdown_sender.send(());
    Ok(())
}
