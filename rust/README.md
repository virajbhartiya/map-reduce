# MapReduce in Rust

This directory contains the Rust implementation of the MapReduce framework. For detailed information about the project, please see the [main README](../README.md).

## Getting Started

### Prerequisites

- Rust (latest stable version)
- Cargo package manager

### Installation

```bash
# Install dependencies
cargo build
```

### Running Examples

```bash
# Run the MapReduce server
cargo run --bin distributed-mapreduce server

# In separate terminals, run workers
cargo run --bin distributed-mapreduce client ./files
```

### Creating Custom MapReduce Tasks

Create a new example in the `examples` directory following the pattern in `examples/custom_functions.rs`.

## Testing

```bash
# Run all tests
cargo test

# Run specific test suite
cargo test --test integration_test
```

## Performance Tips

1. Build in release mode for optimal performance
2. Adjust worker count based on available CPU cores
3. Use appropriate chunk sizes for your data

## License

MIT License
