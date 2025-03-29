use std::time::Duration;

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

        let (sender, mut receiver) = oneshot::channel::<()>();

        // heartbeats
        let fut_heartbeat = {
            let mut client = client.clone();
            async move {
                client.heartbeat(Request::new(async_stream::stream! {
                    let hb = proto::Heartbeat {
                        client_id: Some(client_id.clone()),
                    };

                    while receiver.try_recv().is_err() {
                        yield hb.clone();
                        time::sleep(Duration::from_secs(5)).await;
                    }
                })).await?;
                Ok::<_, anyhow::Error>(())
            }
        };

        let fut_processing = {
            let mut client = client.clone();
            async move {
                let mut tasks = client.get_task(client_id.clone()).await?.into_inner();
                loop {
                    let msg = tasks.message().await?;
                    if let Some(task) = msg {
                        log::info!("task: {:?}", task);
                    } else {
                        log::info!("No more tasks");
                        break;
                    }
                }
                tasks.trailers().await?;
                sender.send(()).unwrap();
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
