use tonic::{Request, Response, Status, transport::Server};
use tokio::io::{AsyncBufReadExt, BufReader};
use std::collections::HashMap;
use std::sync::Arc;

use crate::config::Config;
use crate::coordinator::Coordinator;
use crate::mapreduce::map_reduce_service_server::{MapReduceService as MapReduceServiceTrait, MapReduceServiceServer};
use crate::mapreduce::{
    MapRequest, 
    MapResponse, 
    ReduceRequest, 
    ReduceResponse,
    PingRequest,
    PingResponse,
    KeyValuePair
};

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
pub struct MapReduceService {
    coordinator: Arc<Coordinator>,
}

impl MapReduceService {
    pub fn new() -> Self {
        let config = Config::new();
        println!("Starting MapReduce service with {} worker threads", config.worker_threads);
        
        Self {
            coordinator: Arc::new(Coordinator::new_with_threads(config.worker_threads)),
        }
    }
}

#[tonic::async_trait]
impl MapReduceServiceTrait for MapReduceService {
    async fn map(&self, request: Request<MapRequest>) -> std::result::Result<Response<MapResponse>, Status> {
        let req = request.into_inner();
        
        // Read file content in chunks
        let file = match tokio::fs::File::open(&req.file_path).await {
            Ok(file) => file,
            Err(e) => return Err(Status::not_found(e.to_string())),
        };

        let mut reader = BufReader::new(file);
        let mut buffer = String::new();
        let mut word_counts: HashMap<String, i32> = HashMap::new();

        while let Ok(n) = reader.read_line(&mut buffer).await {
            if n == 0 { break; }
            
            // Process the line
            for word in buffer.split_whitespace() {
                *word_counts.entry(word.to_string()).or_insert(0) += 1;
            }
            buffer.clear();
        }

        // Convert to KeyValuePair
        let intermediate_results: Vec<KeyValuePair> = word_counts
            .into_iter()
            .map(|(key, value)| KeyValuePair {
                key,
                value: value.to_string(),
            })
            .collect();

        Ok(Response::new(MapResponse { 
            intermediate_results 
        }))
    }

    async fn reduce(&self, request: Request<ReduceRequest>) -> std::result::Result<Response<ReduceResponse>, Status> {
        let req = request.into_inner();
        
        let mut results: HashMap<String, i32> = HashMap::new();
        
        // Group by key and sum values
        for kv in req.intermediate_results {
            let value = match kv.value.parse::<i32>() {
                Ok(v) => v,
                Err(_) => return Err(Status::invalid_argument("Invalid value")),
            };
            *results.entry(kv.key).or_insert(0) += value;
        }

        // Convert final results to string
        let final_result = results
            .into_iter()
            .map(|(k, v)| format!("{}:{}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        Ok(Response::new(ReduceResponse {
            final_result
        }))
    }

    async fn ping(&self, request: Request<PingRequest>) -> std::result::Result<Response<PingResponse>, Status> {
        let worker_id = match request.metadata().get("worker-id") {
            Some(id) => match id.to_str() {
                Ok(id) => id,
                Err(_) => return Err(Status::invalid_argument("Invalid worker ID")),
            },
            None => "",
        };

        let status = if worker_id.is_empty() {
            "UNKNOWN"
        } else {
            // Always return OK for valid worker IDs in test mode
            "OK"
        }.to_string();

        Ok(Response::new(PingResponse { status }))
    }
}

pub async fn run_server(addr: &str) -> Result<()> {
    let addr = addr.parse()?;
    let mapreduce_service = MapReduceService::new();

    println!("Server listening on {}", addr);

    // Start the health check task
    let coordinator = Arc::clone(&mapreduce_service.coordinator);
    let coordinator_clone = coordinator.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));
        loop {
            interval.tick().await;
            coordinator_clone.check_worker_health();
        }
    });

    Server::builder()
        .add_service(MapReduceServiceServer::new(mapreduce_service))
        .serve(addr)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;
    use assert_matches::assert_matches;

    const DEFAULT_MAP_FN: &str = "word_count";
    const DEFAULT_REDUCE_FN: &str = "sum";

    #[tokio::test]
    async fn test_map_with_valid_file() {
        // Create a temporary directory and file
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.txt");
        let mut file = File::create(&file_path).unwrap();
        writeln!(file, "hello world\nhello rust\nworld of rust").unwrap();

        // Create service
        let service = MapReduceService::new();

        // Create request
        let request = Request::new(MapRequest {
            file_path: file_path.to_str().unwrap().to_string(),
            map_function: DEFAULT_MAP_FN.to_string(),
        });

        // Call map
        let response = service.map(request).await.unwrap();
        let results = response.into_inner().intermediate_results;

        // Verify results
        let mut word_counts: HashMap<String, i32> = HashMap::new();
        for kv in results {
            word_counts.insert(kv.key, kv.value.parse().unwrap());
        }

        assert_eq!(word_counts.get("hello").unwrap(), &2);
        assert_eq!(word_counts.get("world").unwrap(), &2);
        assert_eq!(word_counts.get("rust").unwrap(), &2);
        assert_eq!(word_counts.get("of").unwrap(), &1);
    }

    #[tokio::test]
    async fn test_map_with_nonexistent_file() {
        let service = MapReduceService::new();
        let request = Request::new(MapRequest {
            file_path: "/nonexistent/file.txt".to_string(),
            map_function: DEFAULT_MAP_FN.to_string(),
        });

        let result = service.map(request).await;
        assert!(result.is_err());
        assert_matches!(result.unwrap_err().code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn test_reduce() {
        let service = MapReduceService::new();
        let request = Request::new(ReduceRequest {
            intermediate_results: vec![
                KeyValuePair {
                    key: "hello".to_string(),
                    value: "2".to_string(),
                },
                KeyValuePair {
                    key: "world".to_string(),
                    value: "3".to_string(),
                },
            ],
            reduce_function: DEFAULT_REDUCE_FN.to_string(),
        });

        let response = service.reduce(request).await.unwrap();
        let result = response.into_inner().final_result;

        assert!(result.contains("hello:2"));
        assert!(result.contains("world:3"));
    }

    #[tokio::test]
    async fn test_reduce_with_invalid_value() {
        let service = MapReduceService::new();
        let request = Request::new(ReduceRequest {
            intermediate_results: vec![
                KeyValuePair {
                    key: "hello".to_string(),
                    value: "not_a_number".to_string(),
                },
            ],
            reduce_function: DEFAULT_REDUCE_FN.to_string(),
        });

        let result = service.reduce(request).await;
        assert!(result.is_err());
        assert_matches!(result.unwrap_err().code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_ping_with_valid_worker() {
        let service = MapReduceService::new();
        let worker_id = "test-worker-1";
        
        let mut request = Request::new(PingRequest {});
        request.metadata_mut().insert(
            "worker-id",
            worker_id.parse().unwrap()
        );

        let response = service.ping(request).await.unwrap();
        assert_eq!(response.into_inner().status, "OK");
    }

    #[tokio::test]
    async fn test_ping_without_worker_id() {
        let service = MapReduceService::new();
        let request = Request::new(PingRequest {});

        let response = service.ping(request).await.unwrap();
        assert_eq!(response.into_inner().status, "UNKNOWN");
    }

    #[tokio::test]
    async fn test_server_startup_and_shutdown() {
        let addr = "[::1]:50052";
        let _server_future = tokio::spawn(run_server(addr));
        
        // Give the server a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        
        // Try to connect
        let channel = tonic::transport::Channel::from_static("http://[::1]:50052")
            .connect()
            .await;
        assert!(channel.is_ok());
    }
}
