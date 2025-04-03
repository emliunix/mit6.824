use std::{collections::{BTreeMap, BinaryHeap, VecDeque}, pin::Pin, sync::Arc, time::Duration};

use futures::{stream, FutureExt, StreamExt};
use tokio::{net::UnixListener, sync::{mpsc, oneshot, Mutex}, time};
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
    workers_heap: BinaryHeap<(usize, usize)>, // (remaining_tasks, worker_index)
}

#[derive(Clone)]
struct MasterServer {
    inner: Arc<Mutex<MasterInner>>,
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
                tokio::select! {
                    Some(task) = receiver.recv() => {
                        yield task;
                    }
                    else => {
                        log::info!("Worker#{} disconnected", id);
                        break;
                    }
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
        loop {
            match hbs.message().await {
                Ok(Some(hb)) => {
                    client_id = hb.client_id;
                    log::trace!("Heartbeat from client: {:?}", client_id);
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
        Ok(Response::new(()))
    }
}

impl MasterServer {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(MasterInner {
            client_seq: 0,
            task_seq: 0,
            task_queue: VecDeque::new(),
            workers: Vec::new(),
            workers_heap: BinaryHeap::new(),
        }));
        Self { inner }
    }
    
    async fn run(&self, shutdown_receiver: oneshot::Receiver<()>) -> Result<(), anyhow::Error> {
        log::info!("Master starting");

        let shutdown_receiver = async move {
            shutdown_receiver.await.unwrap_or(())
        }.shared();
        
        // delete master socket if exists
        let sock = master_sock();
        let _ = std::fs::remove_file(&sock);
        Server::builder()
            .add_service(proto::master_server::MasterServer::new(self.clone()))
            .serve_with_incoming_shutdown(
                UnixListenerStream::new(UnixListener::bind(&sock)?),
                shutdown_receiver.clone(),
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
        let mut had_worker = false;
        {
            let mut inner = self.inner.lock().await;
            had_worker = inner.evict_client_and_requeue(client_id);
        }
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
            let _ = task.oneshot.send(result.clone());
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
        self.workers_heap.push((0, self.workers.len() - 1));
    }

    fn clear_clients(&mut self) {
        self.workers.clear();
        self.workers_heap.clear();
    }

    fn evict_client_and_requeue(&mut self, client_id: i64) -> bool {
        let i = if let Some((i, _)) = self.workers.iter().enumerate().find(|(_, w)| w.id == client_id) { i }
        else { log::warn!("worker#{} not found", client_id); return false; };
        let w = self.workers.remove(i);
        self.workers_heap.retain(|(_, j)| *j != i);
        log::info!("Evicting worker#{} and requeue {} tasks", client_id, w.tasks.len());
        self.task_queue.extend(w.tasks.into_values());
        true
    }

    fn dispatch_task_1(&mut self) -> DispatchResult {
        if let Some(&(_, worker_index)) = self.workers_heap.peek() {
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
                    self.workers_heap.push((ntasks, worker_index));
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
            self.workers_heap.retain(|&(_, j)| j != i);
            self.workers_heap.push((w.tasks.len(), i));
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

    pub async fn run(&self) -> Result<(), anyhow::Error> {
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let server = MasterServer::new();

        let _ = tokio::try_join!(
            server.run(shutdown_receiver),
            master_task(shutdown_sender, server.clone()),
        );
        Ok(())
    }
}

async fn run_tasks<TI>(server: &MasterServer, tasks: TI) -> Result<Vec<proto::task_result::TaskResultType>, anyhow::Error>
where
    TI: IntoIterator<Item = proto::task::TaskType>,
{
    let res = Arc::new(Mutex::new(BTreeMap::new()));
    stream::iter(tasks).enumerate().for_each_concurrent(10, |(i, task)| {
        let res = res.clone();
        let server = server.clone();
        async move {
            let mut res = res.lock().await;
            res.insert(i, server.schedule_task(task).await);
        }
    }).await;
    let res = Arc::try_unwrap(res).unwrap().into_inner();
    let res_len = res.len();
    let mut out = Vec::with_capacity(res_len);
    for v in res.into_values() {
        let v = v?;
        out.push(v);
    }
    Ok(out)
}

async fn master_task(shutdown_sender: oneshot::Sender<()>, server: MasterServer) -> Result<(), anyhow::Error> {
    //     server.schedule_task(task);
    let map_tasks = vec![
        proto::task::TaskType::MapTask(proto::MapTask {
            app: "app".to_string(),
            data: vec![
                proto::KeyValue {
                    key: "key1".to_string(),
                    value: "value1".to_string(),
                },
                proto::KeyValue {
                    key: "key2".to_string(),
                    value: "value2".to_string(),
                },
            ],
        }),
    ];

    run_tasks(&server, map_tasks).await;
    log::info!("All tasks completed, shutting down");
    server.shutdown().await;
    let _ = shutdown_sender.send(());
    Ok(())
}
