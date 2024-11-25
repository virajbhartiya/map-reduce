use distributed_mapreduce::client;
use distributed_mapreduce::server;
use distributed_mapreduce::mapreduce::map_reduce_service_client::MapReduceServiceClient;
use std::fs::File;
use std::io::Write;
use tempfile::tempdir;
use tokio::time::Duration;

const DEFAULT_MAP_FN: &str = "word_count";
const DEFAULT_REDUCE_FN: &str = "sum";

#[tokio::test]
async fn test_end_to_end_mapreduce() {
    // Start server
    let server_addr = "[::1]:50053";
    let server_handle = tokio::spawn(async move {
        server::run_server(server_addr).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test files
    let dir = tempdir().unwrap();
    
    // Create first test file
    let file1_path = dir.path().join("test1.txt");
    let mut file1 = File::create(&file1_path).unwrap();
    writeln!(file1, "hello world\nhello rust").unwrap();

    // Create second test file
    let file2_path = dir.path().join("test2.txt");
    let mut file2 = File::create(&file2_path).unwrap();
    writeln!(file2, "world of rust\nrust programming").unwrap();

    // Run map-reduce job
    let result = client::run_map_reduce_job(
        format!("http://{}", server_addr),
        vec![
            file1_path.to_str().unwrap().to_string(),
            file2_path.to_str().unwrap().to_string(),
        ],
        DEFAULT_MAP_FN.to_string(),
        DEFAULT_REDUCE_FN.to_string(),
    ).await.unwrap();

    // Verify results contain expected word counts
    assert!(result.contains("hello:2"));
    assert!(result.contains("world:2"));
    assert!(result.contains("rust:3"));
    assert!(result.contains("programming:1"));
    assert!(result.contains("of:1"));

    // Cleanup
    server_handle.abort();
}

#[tokio::test]
async fn test_worker_health_tracking() {
    // Start server
    let server_addr = "[::1]:50054";
    let server_handle = tokio::spawn(async move {
        server::run_server(server_addr).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create a client and register as a worker
    let worker_id = "test-worker-1";
    let mut client = MapReduceServiceClient::connect(
        format!("http://{}", server_addr)
    ).await.unwrap();

    // Send initial ping
    let mut request = tonic::Request::new(distributed_mapreduce::mapreduce::PingRequest {});
    request.metadata_mut().insert(
        "worker-id",
        worker_id.parse().unwrap()
    );
    
    let response = client.ping(request).await.unwrap();
    assert_eq!(response.into_inner().status, "OK");

    // Wait for a while without sending pings
    tokio::time::sleep(Duration::from_secs(11)).await;

    // Try ping again - should be treated as a new worker
    let mut request = tonic::Request::new(distributed_mapreduce::mapreduce::PingRequest {});
    request.metadata_mut().insert(
        "worker-id",
        worker_id.parse().unwrap()
    );
    
    let response = client.ping(request).await.unwrap();
    assert_eq!(response.into_inner().status, "OK");

    // Cleanup
    server_handle.abort();
}

#[tokio::test]
async fn test_concurrent_mapreduce_jobs() {
    // Start server
    let server_addr = "[::1]:50055";
    let server_handle = tokio::spawn(async move {
        server::run_server(server_addr).await.unwrap();
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Create test files
    let dir = tempdir().unwrap();
    
    // Create test files for job 1
    let file1_path = dir.path().join("job1.txt");
    let mut file1 = File::create(&file1_path).unwrap();
    writeln!(file1, "hello world").unwrap();

    // Create test files for job 2
    let file2_path = dir.path().join("job2.txt");
    let mut file2 = File::create(&file2_path).unwrap();
    writeln!(file2, "rust programming").unwrap();

    // Run two jobs concurrently
    let (result1, result2) = tokio::join!(
        client::run_map_reduce_job(
            format!("http://{}", server_addr),
            vec![file1_path.to_str().unwrap().to_string()],
            DEFAULT_MAP_FN.to_string(),
            DEFAULT_REDUCE_FN.to_string(),
        ),
        client::run_map_reduce_job(
            format!("http://{}", server_addr),
            vec![file2_path.to_str().unwrap().to_string()],
            DEFAULT_MAP_FN.to_string(),
            DEFAULT_REDUCE_FN.to_string(),
        )
    );

    // Verify results
    let result1 = result1.unwrap();
    let result2 = result2.unwrap();

    assert!(result1.contains("hello:1"));
    assert!(result1.contains("world:1"));
    assert!(result2.contains("rust:1"));
    assert!(result2.contains("programming:1"));

    // Cleanup
    server_handle.abort();
}
