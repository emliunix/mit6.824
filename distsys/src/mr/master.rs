use std::{pin::Pin, sync::Arc};

use tokio::{net::UnixListener, sync::{mpsc, oneshot, Mutex}};
use tokio_stream::{wrappers::UnixListenerStream, Stream};
use tonic::{async_trait, transport::Server, Request, Response, Status, Streaming};
use super::rpc::{master_sock, proto};

struct WorkerConnection {
    id: i64,
    n_processing: i64,
    task_sender: mpsc::Sender<proto::Task>,
}

enum Event {
    TaskDone(i64, i64),
    TaskFailure(i64, i64),
    WorkerEvict(i64),
}

struct MasterInner {
    client_seq: i64,
    event_sender: Option<mpsc::Sender<Event>>,
    workers: Vec<WorkerConnection>,
}

#[derive(Clone)]
pub struct Master {
    inner: Arc<Mutex<MasterInner>>,
}

#[async_trait]
impl proto::master_server::Master for Master {
    async fn gen_client_id(&self, request: Request<()>) -> Result<Response<proto::ClientId>, Status> {
        Ok(Response::new(proto::ClientId { id: 0 }))
    }

    type getTaskStream = Pin<Box<dyn Stream<Item=Result<proto::Task, Status>> + Send + 'static>>;

    async fn get_task(&self, request: Request<proto::ClientId>) -> Result<Response<Self::getTaskStream>, Status> {
        let self_ = self.clone();
        Ok(Response::new(Box::pin(async_stream::try_stream! {
            let client_id = self_.new_client_id().await;
            let (sender, mut receiver)= mpsc::channel::<proto::Task>(10);
            self_.add_client(WorkerConnection {
                id: client_id,
                n_processing: 0,
                task_sender: sender,
            }).await;
            loop {
                tokio::select! {
                    Some(task) = receiver.recv() => {
                        yield task;
                    }
                    else => {
                        log::info!("No more tasks");
                        break;
                    }
                }
            }
        })))
    }

    async fn finish_task(&self, request: Request<proto::FinishTaskRequest>) -> Result<Response<()>, Status> {
        let task = request.into_inner();
        log::info!("Finish task: {:?}", task);
        self.finish_task(task.client_id.unwrap().id, task.task_id, task.is_error, task.error_msg).await;
        Ok(Response::new(()))
    }

    async fn heartbeat(&self, request: Request<Streaming<proto::Heartbeat>>) -> Result<Response<()>, Status> {
        let mut hbs = request.into_inner();
        let mut client_id = None;
        loop {
            match hbs.message().await {
                Ok(Some(hb)) => {
                    client_id = hb.client_id;
                    log::debug!("Heartbeat from client: {:?}", client_id);
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
            self.evict_client(client_id.id).await;
            log::info!("Client {} exited", client_id.id);
        }
        Ok(Response::new(()))
    }
}

impl Master {
    pub fn new() -> Self {
        let inner = Arc::new(Mutex::new(MasterInner {
            client_seq: 0,
            event_sender: None,
            workers: Vec::new(),
        }));
        Self { inner }
    }
    
    pub async fn run(self) -> Result<(), anyhow::Error> {
        log::info!("Master starting");
        // delete master socket if exists
        let sock = master_sock();
        let _ = std::fs::remove_file(&sock);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel::<()>();
        let (res1, res2) = tokio::join!(
            Server::builder()
                .add_service(proto::master_server::MasterServer::new(self))
                .serve_with_incoming_shutdown(
                    UnixListenerStream::new(UnixListener::bind(&sock)?),
                    async move { shutdown_receiver.await.unwrap_or(()) },
                ),
            self.coordinate(shutdown_sender),
        );
        res1?;
        res2?;
        log::info!("Master exiting");
        Ok(())
    }

    async fn coordinate(&self, shutdown_sender: oneshot::Sender<()>) -> Result<(), anyhow::Error> {
        let (event_sender, mut event_receiver) = mpsc::channel::<Event>(10);
        {
            let mut inner = self.inner.lock().await;
            inner.event_sender = Some(event_sender);
        }
        loop {
            tokio::select! {
                Some(event) = event_receiver.recv() => {
                    match event {
                        Event::TaskDone(client_id, task_id) => {
                            log::info!("Task {} done by client {}", task_id, client_id);
                        }
                        Event::TaskFailure(client_id, task_id) => {
                            log::error!("Task {} failed by client {}", task_id, client_id);
                        }
                        Event::WorkerEvict(client_id) => {
                            log::info!("Client {} evicted", client_id);
                        }
                    }
                }
                else => break,
            }
        }
        shutdown_sender.send(()).unwrap();
        Ok(())
    }

    async fn new_client_id(&self) -> i64 {
        let mut inner = self.inner.lock().await;
        inner.client_seq += 1;
        inner.client_seq
    }

    async fn add_client(&self, client: WorkerConnection) {
        let mut inner = self.inner.lock().await;
        inner.workers.push(client);
    }

    async fn remove_client(&self, client_id: i64) {
        let mut inner = self.inner.lock().await;
        inner.workers.retain(|c| c.id != client_id);
    }

    async fn evict_client(&self, client_id: i64) {
        let inner = self.inner.lock().await;
        inner.event_sender.as_ref().unwrap().send(Event::WorkerEvict(client_id)).await.unwrap();
    }

    async fn finish_task(&self, client_id: i64, task_id: i64, is_error: bool, error_msg: Option<String>) {
        let inner = self.inner.lock().await;
        if is_error {
            inner.event_sender.as_ref().unwrap().send(Event::TaskFailure(client_id, task_id)).await.unwrap();
            log::error!("Task {} failed: {:?}", task_id, error_msg);
        } else {
            inner.event_sender.as_ref().unwrap().send(Event::TaskDone(client_id, task_id)).await.unwrap();
            log::info!("Task {} finished", task_id);
        }
    }
}
