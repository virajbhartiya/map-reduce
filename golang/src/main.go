package main

import (
	"fmt"
	"log"
	mr "mapreduce/internal"
	"os"
	"plugin"
	"time"
)

const (
	coordinatorArg = "mrcoordinator"
	workerArg      = "mrworker"
	minArgs        = 3
	sleepDuration  = time.Second
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if len(os.Args) < minArgs {
		return fmt.Errorf("Usage: go run main.go [mrcoordinator|mrworker] [plugin_file] [input_files...]")
	}

	switch os.Args[1] {
	case coordinatorArg:
		return runCoordinator()
	case workerArg:
		return runWorker()
	default:
		return fmt.Errorf("Invalid argument: %s. Use 'mrcoordinator' or 'mrworker'", os.Args[1])
	}
}

func runWorker() error {
	mapf, reducef, err := loadPlugin(os.Args[2])
	if err != nil {
		return err
	}
	mr.Worker(mapf, reducef)
	return nil
}

func runCoordinator() error {
	m := mr.MakeCoordinator(os.Args[2:], 10)
	for !m.Done() {
		time.Sleep(sleepDuration)
	}
	time.Sleep(sleepDuration)
	return nil
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string, error) {
	p, err := plugin.Open(filename)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot load plugin %v: %w", filename, err)
	}

	mapf, err := lookupPluginFunc(p, "Map")
	if err != nil {
		return nil, nil, err
	}

	reducef, err := lookupPluginFunc(p, "Reduce")
	if err != nil {
		return nil, nil, err
	}

	return mapf.(func(string, string) []mr.KeyValue), reducef.(func(string, []string) string), nil
}

func lookupPluginFunc(p *plugin.Plugin, funcName string) (plugin.Symbol, error) {
	symbol, err := p.Lookup(funcName)
	if err != nil {
		return nil, fmt.Errorf("cannot find %s in plugin: %w", funcName, err)
	}
	return symbol, nil
}
