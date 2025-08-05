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

var linesProcessed uint64 = 0

func main() {
	workingDir, err := os.Getwd()
	check(err)
	tempraturesFileName := filepath.Join(workingDir, "dataset", "m2.txt")
	// tempraturesFileName := filepath.Join(workingDir, "dataset", "measurements.txt")
	fileInfo, err := os.Stat(tempraturesFileName)
	check(err)
	file, err := os.Open(tempraturesFileName)
	check(err)
	defer file.Close()
	totalSizeInBytes := fileInfo.Size()
	numCores := runtime.NumCPU()
	fmt.Printf("File size: %+v, total cores: %v\n", totalSizeInBytes, numCores)
	// boundaries := getChunkBoundaries(20, 3)
	boundaries := getChunkBoundaries(totalSizeInBytes, numCores)
	boundaries = adjustChunkBoundaries(boundaries, file)
	// var sum int64 = 0
	// for _, b := range boundaries {
	// 	fmt.Printf("%v - %v (%v bytes)\n", b[0], b[1], b[1]-b[0])
	// 	sum += (b[1] - b[0])
	// }
	// fmt.Printf("Total bytes: %v\n", sum)
	startTime := time.Now()
	wg := &sync.WaitGroup{}
	for _, b := range boundaries {
		wg.Add(1)
		go worker(
			tempraturesFileName,
			b[0],
			b[1],
			wg,
		)
	}
	wg.Wait()
	fmt.Printf("Time taken: %v\n", time.Since(startTime))
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

func worker(filename string, chunkstart, chunkend int64, wg *sync.WaitGroup) {
	defer wg.Done()
	file, err := os.Open(filename)
	check(err)
	defer file.Close()
	file.Seek(chunkstart, io.SeekStart)
	reader := io.LimitedReader{
		R: file,
		N: chunkend - chunkstart,
	}
	buffer := make([]byte, 10*BYTE)
	// buffer := make([]byte, 1*MEGABYTE)
	leftover := []byte{}
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
}

func processBytes(data []byte) {
	lines := bytes.Split(data, []byte{'\n'})
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}
		fmt.Println(string(line))
		// atomic.AddUint64(&linesProcessed, uint64(1))
	}
}
