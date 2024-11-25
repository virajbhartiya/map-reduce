use tonic::Request;
use uuid::Uuid;

use crate::mapreduce::{
    map_reduce_service_client::MapReduceServiceClient,
    MapRequest,
    PingRequest,
    ReduceRequest,
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub struct MapReduceClient {
    client: MapReduceServiceClient<tonic::transport::Channel>,
    worker_id: String,
}

impl MapReduceClient {
    pub async fn new(server_address: String) -> Result<Self> {
        let client = MapReduceServiceClient::connect(server_address).await?;
        let worker_id = Uuid::new_v4().to_string();

        Ok(Self { client, worker_id })
    }

    async fn send_heartbeat(&mut self) -> Result<()> {
        let mut request = Request::new(PingRequest {});
        request
            .metadata_mut()
            .insert("worker-id", self.worker_id.parse().unwrap());

        self.client.ping(request).await?;
        Ok(())
    }
}

pub async fn get_txt_files(dir: &str) -> Result<Vec<String>> {
    let mut txt_files = Vec::new();
    let mut entries = tokio::fs::read_dir(dir).await?;
    
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("txt") {
            if let Some(path_str) = path.to_str() {
                txt_files.push(path_str.to_string());
            }
        }
    }
    
    Ok(txt_files)
}

pub async fn run_map_reduce_job(
    server_addr: String,
    files: Vec<String>,
    map_function: String,
    reduce_function: String,
) -> Result<String> {
    let mut client = MapReduceServiceClient::connect(server_addr).await?;

    let mut all_intermediate_results = Vec::new();

    // Map phase
    for file_path in files {
        let request = Request::new(MapRequest {
            file_path,
            map_function: map_function.clone(),
        });

        let response = client.map(request).await?;
        all_intermediate_results.extend(response.into_inner().intermediate_results);
    }

    // Reduce phase
    let request = Request::new(ReduceRequest {
        intermediate_results: all_intermediate_results,
        reduce_function,
    });

    let response = client.reduce(request).await?;
    Ok(response.into_inner().final_result)
}

pub async fn ping_server(server_address: String) -> Result<()> {
    let mut client = MapReduceClient::new(server_address).await?;
    client.send_heartbeat().await?;
    Ok(())
}
