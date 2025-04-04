use std::{collections::BTreeMap, time::Duration};

use futures::FutureExt;
use tokio::{fs::File, io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, AsyncReadExt, BufReader, Lines}, net::UnixStream, sync::oneshot, time};
use tonic::{transport::Uri, Request};
use tower::service_fn;
use crate::mrapps::get_app;

use super::{rpc::{master_sock, proto}, MRApp};

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

                        let result = match task.task_type.unwrap() {
                            proto::task::TaskType::MapTask(map_task) => {
                                let prefix = format!("map-{}-", client_id.id);
                                let res = run_map(&app, map_task.n_partitions, map_task.input_file, prefix).await?;
                                proto::task_result::TaskResultType::MapTaskResult(res)
                            }
                            proto::task::TaskType::ReduceTask(reduce_task) => {
                                let out_file = format!("mr-out-{}-{}", client_id.id, reduce_task.partition);
                                let res = run_reduce(&app, reduce_task.input_files, out_file).await?;
                                proto::task_result::TaskResultType::ReduceTaskResult(res)
                            }
                        };

                        client.finish_task(proto::FinishTaskRequest {
                            client_id: Some(client_id.clone()),
                            task_id: task.id,
                            is_error: false,
                            error_msg: None,
                            result: Some(proto::TaskResult {
                                id: task.id,
                                task_result_type: Some(result),
                            }),
                        }).await?;
                    } else {
                        log::info!("No more tasks");
                        break;
                    }
                }
                let _ = tasks.trailers().await?;
                let _ = shutdown_sender.send(());
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

trait Decoder<K, V> {
    fn next(&mut self) -> impl Future<Output = Result<Option<(K, V)>, anyhow::Error>>;
}

struct LineDecoder<T>(Lines<T>);

impl<B> Decoder<String, String> for LineDecoder<B>
where
    B: AsyncBufRead + Unpin
{
    async fn next(&mut self) -> Result<Option<(String, String)>, anyhow::Error> {
        match self.0.next_line().await? {
            Some(line) => Ok(Some(("".to_string(), line))),
            None => Ok(None),
        }
    }
}

struct IntermediateDecoder<T>(T);

impl<B> Decoder<String, String> for IntermediateDecoder<B>
where
    B: AsyncBufRead + Unpin
{
    async fn next(&mut self) -> Result<Option<(String, String)>, anyhow::Error> {
        let key_len = self.0.read_u32().await?;
        let value_len = self.0.read_u32().await?;
        if key_len == 0 && value_len == 0 {
            return Ok(None);
        }
        let mut buf = vec![0; key_len.max(value_len) as usize];

        self.0.read_exact(&mut buf[0..key_len as usize]).await?;
        let key = String::from_utf8_lossy(&buf[0..key_len as usize]).to_string();

        self.0.read_exact(&mut buf[0..value_len as usize]).await?;
        let value = String::from_utf8_lossy(&buf[0..value_len as usize]).to_string();

        Ok(Some((key, value)))
    }
}

async fn run_map<A: AsRef<dyn MRApp>>(app: A, n_partitions: i64, input_file: String, out_prefix: String) -> Result<proto::MapTaskResult, anyhow::Error> {
    let app = app.as_ref();

    let out_files = (0..n_partitions).map(|i| (i, format!("{}{}", out_prefix, i))).collect::<Vec<_>>();
    let mut outputs = Vec::with_capacity(n_partitions as usize);
    for (_, out_file) in out_files.iter() {
        outputs.push(File::create(out_file).await?);
    }

    let mut input = LineDecoder(BufReader::new(File::open(input_file).await?).lines());
    while let Some((k, v)) = input.next().await? {
        let kvs = app.map(k, v).await?;
        for kv in kvs {
            let part = crate::util::hash(kv.key.as_str()) % n_partitions as u32;
            
            let o = &mut outputs[part as usize];
            let key = kv.key.into_bytes();
            let value = kv.value.into_bytes();
            o.write_u32(key.len() as u32).await?;
            o.write_u32(value.len() as u32).await?;
            o.write_all(&key).await?;
            o.write_all(&value).await?;
        }
    }

    for mut out in outputs {
        out.write_u32(0).await?;
        out.write_u32(0).await?;
        out.flush().await?;
    }

    Ok(proto::MapTaskResult {
        outputs: out_files.into_iter()
            .map(|(i, f)| {
                proto::MapTaskResultEntry {
                    partition: i,
                    output_file: f,
                }
            })
            .collect::<Vec<_>>(),
    })
}

async fn run_reduce<A: AsRef<dyn MRApp>>(app: A, input_files: Vec<String>, out_file: String) -> Result<proto::ReduceTaskResult, anyhow::Error> {
    let app = app.as_ref();
    let mut data = BTreeMap::<String, Vec<String>>::new();
    for input_file in input_files {
        let mut input = IntermediateDecoder(BufReader::new(File::open(input_file).await?));
        while let Some((k, v)) = input.next().await? {
            data.entry(k).or_insert_with(Vec::new).push(v);
        }
    }
    let mut out = File::create(out_file.as_str()).await?;
    for (key, values) in data.into_iter() {
        let res = app.reduce(key.clone(), values).await?;
        out.write_all(format!("{} {}\n", key, res).as_bytes()).await?;
    }
    out.flush().await?;

    Ok(proto::ReduceTaskResult {
        output_files: vec![out_file],
    })
}
