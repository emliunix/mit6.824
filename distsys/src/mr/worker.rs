use std::time::Duration;

use futures::FutureExt;
use tokio::{net::UnixStream, sync::oneshot, time};
use tonic::{transport::Uri, Request};
use tower::service_fn;
use crate::mrapps::get_app;

use super::rpc::{master_sock, proto};

pub struct Worker;

impl Worker {
    pub async fn run(self, app: String) -> Result<(), anyhow::Error> {
        let sock_path = master_sock();
        let conn = tonic::transport::Endpoint::from_static("http://[::1]:50051")
            .connect_with_connector(service_fn(move |_: Uri| {
                let sock_path = sock_path.clone();
                async {
                    let connector = UnixStream::connect(sock_path).await?;
                    let io = hyper_util::rt::TokioIo::new(connector);
                    Ok::<_, anyhow::Error>(io)
                }
            }))
            .await?;
        let mut client = proto::master_client::MasterClient::new(conn);

        let app = get_app(app)?;

        let client_id = client.gen_client_id(()).await?.into_inner();
        log::info!("Assigned client id: {:?}", client_id.id);

        let (shutdown_sender, shutdown) = oneshot::channel::<()>();
        let shutdown = async move {
            shutdown.await.unwrap_or(());
        }.shared();

        // heartbeats
        let fut_heartbeat = {
            let mut client = client.clone();
            async move {
                client.heartbeat(Request::new(async_stream::stream! {
                    let hb = proto::Heartbeat {
                        client_id: Some(client_id.clone()),
                    };

                    log::info!("Sending heartbeat");
                    loop {
                        log::debug!("tick heartbeat");
                        yield hb;
                        tokio::select! {
                            _ = time::sleep(Duration::from_secs(5)) => {}
                            _ = shutdown.clone() => { return; }
                        }
                    }
                })).await?;
                Ok::<_, anyhow::Error>(())
            }
        };

        let fut_processing = {
            let mut client = client.clone();
            async move {
                let mut tasks = client.get_task(proto::GetTaskRequest { client_id: Some(client_id.clone()) }).await?.into_inner();
                log::info!("Receiving tasks stream");
                loop {
                    let msg = tasks.message().await?;
                    if let Some(task) = msg {
                        log::info!("task: {:?}", task);
                        time::sleep(Duration::from_secs(1)).await; // Simulate task processing
                        client.finish_task(proto::FinishTaskRequest {
                            client_id: Some(client_id.clone()),
                            task_id: task.id,
                            is_error: false,
                            error_msg: None,
                            result: Some(proto::TaskResult {
                                id: task.id,
                                task_result_type: Some(proto::task_result::TaskResultType::MapTaskResult(proto::MapTaskResult {
                                    output_files: vec![],
                                })),
                            }),
                        }).await?;
                    } else {
                        log::info!("No more tasks");
                        break;
                    }
                }
                tasks.trailers().await?;
                shutdown_sender.send(()).unwrap();
                Ok::<_, anyhow::Error>(())
            }
        };

        let (res1, res2) = tokio::join!(fut_heartbeat, fut_processing);
        res1?;
        res2?;
        log::info!("Worker exiting");
        Ok(())
    }
}
