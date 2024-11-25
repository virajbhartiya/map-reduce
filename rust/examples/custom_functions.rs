use distributed_mapreduce::functions::{MapFunction, ReduceFunction};
use std::fs::File;
use std::io::Write;
use chrono::Local;
use colored::*;

// Custom map function that counts character frequencies
pub struct CharFrequencyMapper;

impl MapFunction for CharFrequencyMapper {
    fn map(&self, input: &str) -> Vec<(String, String)> {
        input
            .chars()
            .filter(|c| !c.is_whitespace())
            .map(|c| (c.to_string(), "1".to_string()))
            .collect()
    }
}

// Custom reduce function that finds the maximum value
pub struct MaxReducer;

impl ReduceFunction for MaxReducer {
    fn reduce(&self, _key: &str, values: Vec<String>) -> String {
        values
            .iter()
            .filter_map(|v| v.parse::<i64>().ok())
            .max()
            .unwrap_or(0)
            .to_string()
    }
}

fn save_results(result: &str, input_files: &[String]) -> std::io::Result<String> {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let output_file = format!("mapreduce_result_{}.txt", timestamp);
    let mut file = File::create(&output_file)?;

    writeln!(file, "Map-Reduce Job Results")?;
    writeln!(file, "====================")?;
    writeln!(file, "\nTimestamp: {}", Local::now().format("%Y-%m-%d %H:%M:%S"))?;
    writeln!(file, "\nInput Files:")?;
    for (i, file_path) in input_files.iter().enumerate() {
        writeln!(file, "{}. {}", i + 1, file_path)?;
    }
    writeln!(file, "\nMap Function: Character Frequency Counter")?;
    writeln!(file, "Reduce Function: Maximum Value")?;
    writeln!(file, "\nResult: {}", result)?;

    Ok(output_file)
}


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use distributed_mapreduce::client;
    use distributed_mapreduce::functions::FunctionRegistry;

    println!("\n{}", "Starting Map-Reduce Job...".blue().bold());

    let mut registry = FunctionRegistry::new();

    // Register custom functions
    registry.register_map_function("char_freq".to_string(), Box::new(CharFrequencyMapper));
    registry.register_reduce_function("max".to_string(), Box::new(MaxReducer));

    // Example usage
    let txt_files = vec!["example.txt".to_string()];

    let result = client::run_map_reduce_job(
        "http://localhost:50051".to_string(),
        txt_files.clone(),
        "char_freq".to_string(),
        "max".to_string(),
    )
    .await?;

    // Save results to file
    let output_file = save_results(&result, &txt_files)?;

    println!("\n{}", "Map-Reduce Job Completed".green().bold());
    println!("\nResults saved to: {}", output_file);
    Ok(())
}
