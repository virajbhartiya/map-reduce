# MapReduce in Go

This directory contains the Go implementation of the MapReduce framework. For detailed information about the project, please see the [main README](../README.md).

## Getting Started

### Prerequisites

- Go 1.16 or later
- Make (optional, for using Makefile commands)


### Running a WordCount Example

1. Build the main application:
   ```bash
   go build -o mr_main src/main.go
   ```

2. Build the WordCount plugin:
   ```bash
   go build -buildmode=plugin -o apps/wordcount.so apps/wordcount.go
   ```

3. Run workers:
   ```bash
   ./mr_main mrworker apps/wordcount.so & ./mr_main mrworker apps/wordcount.so
   ```

4. Run coordinator:
   ```bash
   ./mr_main mrcoordinator files/pg-*
   ```

### Creating Custom MapReduce Tasks

To create your own MapReduce task, implement the Map and Reduce functions in a new Go file, following the pattern in `apps/wordcount.go`.

## Testing

```bash
go test ./...
```

## License

MIT License
