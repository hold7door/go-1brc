package main

import (
	"bufio"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

type Stats struct {
	Min, Mean, Max float64
	Count          int
}

func main() {
	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	linesChan := make(chan string, 1000000)
	resultsChan := make(chan map[string]Stats, numWorkers)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		// A WaitGroup waits for a collection of goroutines to finish.
		// The main goroutine calls Add to set the number of goroutines to wait for
		wg.Add(1)
		go worker(linesChan, resultsChan, &wg)
	}

	// Read lines from file and send to the workers
	go func() {
		file, err := os.Open("measurements.txt")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			linesChan <- scanner.Text()
		}
		close(linesChan)
	}()

	// Collect results from workers
	wg.Wait()
	close(resultsChan)

	finalResults := make(map[string]Stats)

	// Aggregate results
	for workerResult := range resultsChan {
		for station, stats := range workerResult {
			finalStats, exists := finalResults[station]
			if !exists {
				finalStats = stats
			} else {
				finalStats.Min = min(finalStats.Min, stats.Min)
				finalStats.Max = max(finalStats.Max, stats.Max)
				finalStats.Mean = (finalStats.Mean*float64(finalStats.Count) + stats.Mean*float64(stats.Count)) / float64(finalStats.Count+stats.Count)
				finalStats.Count += stats.Count
			}
			finalResults[station] = finalStats
		}
	}
	printStats(finalResults)
}

func worker(linesChan <-chan string, resultsChan chan<- map[string]Stats, wg *sync.WaitGroup) {
	defer wg.Done()

	stationStats := make(map[string]Stats)

	for line := range linesChan {
		// process line
		parts := strings.Split(line, ";")
		temp, err := strconv.ParseFloat(parts[1], 64)
		if err != nil {
			continue
		}
		stats, exists := stationStats[parts[0]]
		if !exists {
			stats = Stats{
				Min: temp,
				Max: temp,
			}
		}
		stats.Count++
		if temp < stats.Min {
			stats.Min = temp
		}
		if temp > stats.Max {
			stats.Max = temp
		}
		stats.Mean += (temp - stats.Mean) / float64(stats.Count)
		stationStats[parts[0]] = stats
	}

	resultsChan <- stationStats
}

func min(a, b float64) float64 {
	if a == 0 || a > b {
		return b
	}
	return a
}

func max(a, b float64) float64 {
	if a < b {
		return b
	}
	return a
}

func printStats(stats map[string]Stats) {
	for station, stat := range stats {
		println(station, stat.Min, stat.Max, stat.Mean)
	}
}
