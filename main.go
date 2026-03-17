package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// PingResult stores the data for each check
type PingResult struct {
	Host     	string
	Status   	string
	Response 	time.Duration
	Try      	int
	MaxTries 	int
	Timestamp 	time.Time
}

func main() {
	// 1. Parsing Flags
	filePath 	:= flag.String("f", "", "Path to the hosts file")
	count 		:= flag.Int("c", 1, "Number of retries (for one-time check mode)")
	monitor 	:= flag.Bool("monitor", false, "Enable continuous monitoring mode")
	flag.Parse()

	if *filePath == "" {
		log.Fatal("Error: Host file is required. Use -f flag.")
	}

	hosts, err 	:= readHosts(*filePath)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	// 2. Execution Logic
	if *monitor {
		runMonitoringMode(hosts)
	} else {
		runOnceMode(hosts, *count)
	}
}

// pingHost performs a TCP connection attempt to port 443
func pingHost(host string) (bool, time.Duration, error) {
	start := time.Now()
	// Using 2s timeout
	conn, err := net.DialTimeout("tcp", host+":443", 2*time.Second)
	duration := time.Since(start)

	if err != nil {
		return false, 0, err
	}
	conn.Close()
	return true, duration, nil
}

// readHosts parses the input file line by line
func readHosts(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var hosts []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if text := scanner.Text(); text != "" {
			hosts = append(hosts, text)
		}
	}
	return hosts, scanner.Err()
}

// Mode 1: One-time check
func runOnceMode(hosts []string, count int) {
	fmt.Printf("%-20s\t%-10s\t%-10s\t%-5s\n", "Host", "Status", "Response", "Try")
	
	for i := 1; i <= count; i++ {
		for _, host := range hosts {
			success, latency, _ := pingHost(host)
			status := "OK"
			if !success {
				status = "Timeout"
			}
			fmt.Printf("%-20s\t%-10s\t%-10v\t%d/%d\n", 
				host, status, latency.Round(time.Millisecond), i, count)
		}
		if i < count {
			time.Sleep(1 * time.Second)
		}
	}
}

// Mode 2: Continuous monitoring
func runMonitoringMode(hosts []string) {
	logFile, err := os.OpenFile("monitor.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	results := make(chan PingResult)
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// Start a worker goroutine for each host
	for _, host := range hosts {
		wg.Add(1)
		go func(h string) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					success, latency, _ := pingHost(h)
					status := "OK"
					if !success {
						status = "FAIL"
					}
					results <- PingResult{
						Host:      h,
						Status:    status,
						Response:  latency,
						Timestamp: time.Now(),
					}
					time.Sleep(5 * time.Second) // Default monitoring interval
				}
			}
		}(host)
	}

	// Result Listener (Console & File output)
	go func() {
		for res := range results {
			logLine := fmt.Sprintf("%s | %-20s | %-5s | %v", 
				res.Timestamp.Format("2006-01-02 15:04:05"), 
				res.Host, res.Status, res.Response.Round(time.Millisecond))
			
			fmt.Println(logLine)
			logFile.WriteString(logLine + "\n")
		}
	}()

	// Graceful Shutdown handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	// Setup work here

	<-sigChan

	fmt.Println("\nShutting down... Waiting for workers to finish.")
	close(stop)    // Signals workers to stop
	wg.Wait()      // Waits for all goroutines to finish
	close(results) // Closes channel to finish the listener
	fmt.Println("PingMonitor stopped.")
}
