package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// const (
// 	FNAME    = "m2.txt"
// 	BUFFSIZE = 10 * BYTE
// )

const (
	FNAME    = "measurements.txt"
	BUFFSIZE = 10 * MEGABYTE
)

var linesProcessed uint64 = 0

type CityTemp struct {
	Temp  int64
	Count int64
	Mean  float64
}

type Result struct {
	mu *sync.RWMutex
	mp map[string]*CityTemp
}

func main() {
	workingDir, err := os.Getwd()
	check(err)
	tempraturesFileName := filepath.Join(workingDir, "dataset", FNAME)
	fileInfo, err := os.Stat(tempraturesFileName)
	check(err)
	file, err := os.Open(tempraturesFileName)
	check(err)
	defer file.Close()
	totalSizeInBytes := fileInfo.Size()
	numWorkers := runtime.NumCPU()
	// numWorkers = 8
	fmt.Printf("File size: %+v, total workers: %v, readbuf per worker: %v\n", totalSizeInBytes, numWorkers, BUFFSIZE)
	// boundaries := getChunkBoundaries(20, 3)
	boundaries := getChunkBoundaries(totalSizeInBytes, numWorkers)
	boundaries = adjustChunkBoundaries(boundaries, file)
	// var sum int64 = 0
	// for _, b := range boundaries {
	// 	fmt.Printf("%v - %v (%v bytes)\n", b[0], b[1], b[1]-b[0])
	// 	sum += (b[1] - b[0])
	// }
	// fmt.Printf("Total bytes: %v\n", sum)
	startTime := time.Now()
	wg := &sync.WaitGroup{}
	result := &Result{
		mu: &sync.RWMutex{},
		mp: make(map[string]*CityTemp),
	}
	for _, b := range boundaries {
		wg.Add(1)
		go func() {
			defer wg.Done()
			mp := worker(
				tempraturesFileName,
				b[0],
				b[1],
			)
			for city, values := range mp {
				result.mu.Lock()
				cur, ok := result.mp[city]
				if !ok {
					result.mp[city] = values
					result.mu.Unlock()
					continue
				}
				cur.Temp += values.Temp
				cur.Count += values.Count
				result.mu.Unlock()
			}
		}()
	}

	wg.Wait()

	for _, value := range result.mp {
		value.Mean = float64(value.Temp) / (10 * float64(value.Count))
	}

	fmt.Printf("Time taken: %v\n", time.Since(startTime))
	// for city, value := range result.mp {
	// 	fmt.Printf("City: %s, Mean Temperature: %.2f\n", city, value.Mean)
	// }
	// fmt.Printf("total lines processed: %v\n", linesProcessed)
}

func getChunkBoundaries(totalsize int64, numChunks int) [][2]int64 {
	boundaries := [][2]int64{}
	chunkSize := totalsize / int64(numChunks)
	var offset int64 = 0
	for i := range numChunks {
		var chunkStart int64 = offset
		var chunkEnd int64 = chunkStart + chunkSize
		if numChunks-1 == i {
			chunkEnd = totalsize
		}
		boundaries = append(boundaries, [2]int64{chunkStart, chunkEnd})
		offset = chunkEnd
	}
	return boundaries
}

func adjustChunkBoundaries(boundaries [][2]int64, file *os.File) [][2]int64 {
	adjusted := make([][2]int64, len(boundaries))

	for i, boundary := range boundaries {
		start := boundary[0]
		end := boundary[1]

		// for chunks after first, use adjusted end of previous chunk
		if i > 0 {
			start = adjusted[i-1][1]
		}

		// skip adjustment for last chunk
		if i == len(boundaries)-1 {
			adjusted[i] = [2]int64{start, end}
			continue
		}

		// check if we're at newline
		file.Seek(end, 0)
		buf := make([]byte, 1)
		file.Read(buf)

		if buf[0] != '\n' {
			// find next newline
			for {
				file.Read(buf)
				end++
				if buf[0] == '\n' {
					break
				}
			}
		}

		adjusted[i] = [2]int64{start, end + 1} // +1 to include the \n
	}

	return adjusted
}

func worker(filename string, chunkstart, chunkend int64) map[string]*CityTemp {
	file, err := os.Open(filename)
	check(err)
	defer file.Close()
	file.Seek(chunkstart, io.SeekStart)
	reader := io.LimitedReader{
		R: file,
		N: chunkend - chunkstart,
	}
	buffer := make([]byte, BUFFSIZE)
	leftover := []byte{}
	mp := make(map[string]*CityTemp)

	processBytes := func(data []byte) {
		lines := bytes.Split(data, []byte{'\n'})
		for _, line := range lines {
			if len(line) == 0 {
				continue
			}
			// temprature formats:
			// XX.X, X.X, -XX.X, -X.X
			end := len(line)
			idx := -1
			negative := false
			for i := end - 4; i > end-7; i-- {
				if line[i] == '-' {
					negative = true
					continue
				}
				if line[i] == ';' {
					idx = i
					break
				}
			}
			if idx == -1 {
				panic("line without ';' found: " + string(line))
			}
			cityName := string(line[:idx])
			var temperature int64 = 0
			temperature += int64(line[end-1] - '0')
			temperature += int64((line[end-3] - '0') * 10)
			if line[end-4] >= '0' && line[end-4] <= '9' {
				temperature += int64((line[end-4] - '0') * 100)
			}
			if negative {
				temperature = temperature * -1
			}
			// fmt.Printf("City: %s, Temp: %d\n", cityName, temperature)
			cur, ok := mp[cityName]
			if !ok {
				cur = &CityTemp{
					Temp:  temperature,
					Count: 1,
				}
				mp[cityName] = cur
				continue
			}
			cur.Temp += temperature
			cur.Count++
		}
	}

	for {
		bytesRead, err := reader.Read(buffer)
		if bytesRead == 0 {
			break
		}

		data := append(leftover, buffer[:bytesRead]...)
		newLineIdx := bytes.LastIndexByte(data, '\n')
		if newLineIdx == -1 {
			leftover = make([]byte, len(data))
			copy(leftover, data)
			continue
		}
		completeLines := data[:newLineIdx+1]
		processBytes(completeLines)
		leftover = make([]byte, len(data)-newLineIdx-1)
		copy(leftover, data[newLineIdx+1:])
		if err != nil {
			if err == io.EOF {
				break
			}
			check(err)
		}
	}
	if len(leftover) > 0 {
		processBytes(leftover)
	}

	return mp
}
