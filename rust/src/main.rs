use chrono::Local;
use colored::Colorize;
use distributed_mapreduce::client;
use distributed_mapreduce::server;
use std::fs::File;
use std::io::Write;

type Error = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, Error>;

// Default map and reduce functions
const DEFAULT_MAP_FN: &str = "word_count";
const DEFAULT_REDUCE_FN: &str = "sum";

fn save_results(
    result: &str,
    input_files: &[String],
    map_fn: &str,
    reduce_fn: &str,
) -> std::io::Result<String> {
    let timestamp = Local::now().format("%Y%m%d_%H%M%S");
    let output_file = format!("mapreduce_result_{}.txt", timestamp);
    let mut file = File::create(&output_file)?;

    writeln!(file, "Map-Reduce Job Results for top 20 words")?;
    writeln!(file, "====================")?;
    writeln!(
        file,
        "\nTimestamp: {}",
        Local::now().format("%Y-%m-%d %H:%M:%S")
    )?;
    writeln!(file, "\nInput Files:")?;
    for (i, file_path) in input_files.iter().enumerate() {
        writeln!(file, "{}. {}", i + 1, file_path)?;
    }
    writeln!(file, "\nMap Function: {}", map_fn)?;
    writeln!(file, "Reduce Function: {}", reduce_fn)?;
    writeln!(file, "\nResults for all words:")?;

    // Parse and format the results
    let results: Vec<_> = result.split(", ").filter(|s| !s.is_empty()).collect();

    for result_pair in results {
        if let Some((word, count)) = result_pair.split_once(':') {
            writeln!(file, "{:<30} : {}", word, count)?;
        }
    }

    Ok(output_file)
}

fn print_job_info(files: &[String], server_addr: &str, map_fn: &str, reduce_fn: &str) {
    println!("\n{}", "Map-Reduce Job Configuration".green());
    println!("Server: {}", server_addr.yellow());
    println!("Map Function: {}", map_fn.yellow());
    println!("Reduce Function: {}", reduce_fn.yellow());
    println!("\nProcessing {} files:", files.len());
    for file in files {
        println!("  • {}", file);
    }
}

fn print_results_graph(result: &str, output_file: &str) {
    // Parse results into (word, count) pairs
    let mut results: Vec<(String, i32)> = result
        .split(", ")
        .filter(|s| !s.is_empty())
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(':').collect();
            if parts.len() == 2 {
                if let Ok(count) = parts[1].trim().parse::<i32>() {
                    Some((parts[0].to_string(), count))
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect();

    // Sort by count in descending order
    results.sort_by(|a, b| b.1.cmp(&a.1));

    // Calculate statistics
    let total_words: i32 = results.iter().map(|(_, count)| count).sum();

    // Take top 20 results for the graph (use iter() instead of into_iter())
    let top_results: Vec<_> = results.iter().take(20).collect();

    if !top_results.is_empty() {
        println!("\nTop 20 words by frequency:");
        // Table header
        println!(
            "{:<8} {:<15} {:>8} {:>9} {}",
            "Rank".blue(),
            "Word".blue(),
            "Count".blue(),
            "Percent".blue(),
            "Distribution".blue()
        );
        println!("{}", "─".repeat(70).white());

        // Calculate maximum percentage for scaling
        let max_percentage = (top_results[0].1 as f32 / total_words as f32) * 100.0;
        let scale_factor = 15.0 / max_percentage;

        // Print each word with a scaled bar
        for (i, (word, count)) in top_results.iter().enumerate() {
            let percentage = (*count as f32 / total_words as f32) * 100.0;
            let bar_length = (percentage * scale_factor) as usize;
            let bar = "█".repeat(bar_length);

            println!(
                "{:<8} {:<15} {:>8} {:>8.2}% {}",
                (i + 1).to_string().yellow(),
                word.cyan(),
                count.to_string().yellow(),
                percentage,
                bar.blue()
            );
        }

        // Output details
        println!("\n{}", "Output Details".blue().bold());
        println!("{}", "─".repeat(50).blue());
        println!("File: {}", output_file.cyan());
        println!("Total unique words: {}", results.len().to_string().yellow());
        println!("Total word count: {}", total_words.to_string().yellow());
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("\n{}", "Usage:".yellow());
        eprintln!("  Server mode: {} server", args[0]);
        eprintln!(
            "  Client mode: {} client <directory> [server_address]",
            args[0]
        );
        eprintln!();
        eprintln!("{}", "Arguments:".yellow());
        eprintln!("  server          Start in server mode");
        eprintln!("  client          Start in client mode");
        eprintln!("  directory       Directory containing txt files to process");
        eprintln!("  server_address  Optional server address (default: http://localhost:50051)");
        std::process::exit(1);
    }

    match args[1].as_str() {
        "server" => {
            println!("\n{}", "Starting Map-Reduce Server...".blue());
            server::run_server("0.0.0.0:50051").await?;
        }
        "client" => {
            if args.len() < 3 {
                eprintln!("{}", "Error: directory path required for client mode".red());
                std::process::exit(1);
            }

            let directory = &args[2];
            let server_addr = if args.len() > 3 {
                args[3].clone()
            } else {
                "http://localhost:50051".to_string()
            };

            println!("\n{}", "Starting Map-Reduce Job...".blue());

            let txt_files = client::get_txt_files(directory).await?;

            if txt_files.is_empty() {
                println!("{}", "No .txt files found in directory".yellow());
                return Ok(());
            }

            print_job_info(&txt_files, &server_addr, DEFAULT_MAP_FN, DEFAULT_REDUCE_FN);

            println!("\n{}", "Processing files...".blue());
            let start_time = std::time::Instant::now();

            let result = client::run_map_reduce_job(
                server_addr,
                txt_files.clone(),
                DEFAULT_MAP_FN.to_string(),
                DEFAULT_REDUCE_FN.to_string(),
            )
            .await?;

            let elapsed_time = start_time.elapsed();
            let output_file = save_results(&result, &txt_files, DEFAULT_MAP_FN, DEFAULT_REDUCE_FN)?;

            println!("\n{}", "Job Completed Successfully!".green());
            print_results_graph(&result, &output_file);
            println!(
                "Total files processed: {}",
                txt_files.len().to_string().yellow()
            );
            println!("Total time: {}", format!("{:.2?}", elapsed_time).cyan());
        }
        _ => {
            eprintln!("{}", "Invalid mode. Use 'server' or 'client'".red());
            std::process::exit(1);
        }
    }

    Ok(())
}
