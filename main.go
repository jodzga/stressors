package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"databricks.com/cpuhist/histutils"
	"databricks.com/cpuhist/randutils"
)

var (
	mode                            string
	diskPath                        string
	frequency                       int
	displaySeconds                  int
	histogram                       *histutils.Histogram
	totalCPUHistogram               *histutils.Histogram
	totalWorkHistogram              *histutils.Histogram
	latestCPUTime                   int
	mtx                             sync.Mutex
	numCPU                          float64
	rate                            float64
	alpha                           float64
	xm                              float64
	paretoAvg                       float64
	workWeight                      float64
	targetCPUPct                    int
	mtx2                            sync.Mutex
	regressionHistoryLength         int
	X                               []float64
	Y                               []float64
	workSheet                       []byte
	arrivalDistribution             string
	workDistribution                string
	cpuSamplesOutputFileName        string
	cpuHistogramOutputFilenema      string
	workHistogramOutputFilenema     string
	cpuUtilizationOutputFileName    string
	perCpuUtilizationOutputFileName string
	maxWorkParallelism              int
	workParallelismDistribution     string
	mtxStopped                      sync.Mutex
	stopped                         bool = false
	maxWork                         float64
	finalSleepTime                  time.Duration
	// collectedAvgCPUSamples  int = 0
)

const GENERATE_REQUESTS_INTERVAL = 5 * time.Second
const GENERATE_REQUESTS_BREAK = GENERATE_REQUESTS_INTERVAL / 2

type WorkItem struct {
	// The time at which the work should start
	startTime time.Time
	// amount of work to do
	weight float64
}

type Worker struct {
	workHistogram *histutils.Histogram
}

func init() {
	flag.StringVar(&mode, "m", "cpu", "Mode (cpu or disk)")
	flag.StringVar(&diskPath, "dp", "/tmp", "Path to write files to for disk mode")
	flag.IntVar(&frequency, "f", 27, "Frequency to read /proc/stat")
	flag.IntVar(&displaySeconds, "d", 1, "Interval to emit histogram in seconds")
	flag.Float64Var(&rate, "r", 100, "Rate of work to schedule in Hz (work per second) - Poisson distribution")
	flag.Float64Var(&alpha, "a", 1.16, "Alpha for Pareto distribution")
	flag.Float64Var(&xm, "x", 1, "Xm for Pareto distribution")
	flag.IntVar(&targetCPUPct, "t", 40, "Target CPU usage percentage")
	flag.IntVar(&regressionHistoryLength, "h", 6, "Number of samples to use for regression")
	flag.StringVar(&arrivalDistribution, "ad", "poisson", "Arrival distribution (poisson or constant)")
	flag.StringVar(&workDistribution, "wd", "pareto", "Work distribution (pareto or constant)")
	flag.Float64Var(&workWeight, "ww", 15000000, "Weight of work to do in each event")
	flag.StringVar(&cpuSamplesOutputFileName, "o", "cpu_samples.csv", "Output file name")
	flag.StringVar(&cpuHistogramOutputFilenema, "oh", "total_cpu_histogram.csv", "Output file name for CPU histogram")
	flag.StringVar(&workHistogramOutputFilenema, "ow", "total_work_histogram.csv", "Output file name for work histogram")
	flag.StringVar(&cpuUtilizationOutputFileName, "ou", "cpu_utilization.csv", "Output file name for CPU utilization")
	flag.IntVar(&maxWorkParallelism, "p", 16, "Maximum parallelism of workers")
	flag.StringVar(&workParallelismDistribution, "pd", "exponential", "Work parallelism distribution (constant, uniform or exponential)")
	flag.StringVar(&perCpuUtilizationOutputFileName, "oc", "per_cpu_utilization.csv", "Output file name for per CPU utilization")
	flag.Float64Var(&maxWork, "mw", 15000000000, "Maximum work to do in each event")
	flag.DurationVar(&finalSleepTime, "s", 0*time.Second, "Final sleep time")
}

func main() {
	flag.Parse()

	// print out all the parameters
	fmt.Printf("mode: %s\n", mode)
	fmt.Printf("diskPath: %s\n", diskPath)
	fmt.Printf("frequency: %d\n", frequency)
	fmt.Printf("displaySeconds: %d\n", displaySeconds)
	fmt.Printf("rate: %f\n", rate)
	fmt.Printf("alpha: %f\n", alpha)
	fmt.Printf("xm: %f\n", xm)
	fmt.Printf("targetCPUPct: %d\n", targetCPUPct)
	fmt.Printf("regressionHistoryLength: %d\n", regressionHistoryLength)
	fmt.Printf("arrivalDistribution: %s\n", arrivalDistribution)
	fmt.Printf("workDistribution: %s\n", workDistribution)
	fmt.Printf("workWeight: %f\n", workWeight)
	fmt.Printf("cpuSamplesOutputFileName: %s\n", cpuSamplesOutputFileName)
	fmt.Printf("cpuHistogramOutputFilenema: %s\n", cpuHistogramOutputFilenema)
	fmt.Printf("workHistogramOutputFilenema: %s\n", workHistogramOutputFilenema)
	fmt.Printf("cpuUtilizationOutputFileName: %s\n", cpuUtilizationOutputFileName)
	fmt.Printf("maxWorkParallelism: %d\n", maxWorkParallelism)
	fmt.Printf("workParallelismDistribution: %s\n", workParallelismDistribution)
	fmt.Printf("perCpuUtilizationOutputFileName: %s\n", perCpuUtilizationOutputFileName)
	fmt.Printf("maxWork: %f\n", maxWork)
	fmt.Printf("finalSleepTime: %s\n", finalSleepTime)

	if mode == "disk" {
		fmt.Printf("Disk mode\n")

		testFile := diskPath + "/test_file"

		createLargeFile(testFile)

		// Memory-map the file

		modifyMappedFile(testFile)

	} else if mode == "cpu" {
		fmt.Printf("CPU mode\n")

		paretoAvg = xm * alpha / (alpha - 1)

		// initialize work sheet with 10MB of random data
		workSheet = make([]byte, 10000000)
		rand.Read(workSheet)

		// initialize X and Y for linear regression with empty arrays
		X = make([]float64, regressionHistoryLength)
		Y = make([]float64, regressionHistoryLength)

		rand.Seed(time.Now().UnixNano())

		numCPU = float64(runtime.NumCPU())
		if maxWorkParallelism == 0 || maxWorkParallelism > int(numCPU) {
			maxWorkParallelism = int(numCPU)
		}

		histogram = histutils.NewHistogram("equal", 1.0, 100.0)
		totalCPUHistogram = histutils.NewHistogram("equal", 1.0, 100.0)

		totalWorkHistogram = histutils.NewHistogram("exponential", 2.0, 1e9)

		// Create a done channel to signal workers to stop
		done := make(chan struct{})

		go updateCPUHistogram(done)

		var cpuSummaryWg sync.WaitGroup
		cpuSummaryWg.Add(1)
		go updateCPUSummary(done, &cpuSummaryWg)

		// Create the channel for the messages
		// Buffer size set to handle worse-case scenario
		bufferSize := int(math.Ceil(1.5 * numCPU * rate * GENERATE_REQUESTS_INTERVAL.Seconds()))
		c := make(chan WorkItem, bufferSize)
		go scheduleWork(rate, c, done)

		// make sure we use all available CPUs
		runtime.GOMAXPROCS(int(numCPU))

		// Create a WaitGroup to wait for all the workers to finish
		var wg sync.WaitGroup

		workers := make([]*Worker, int(numCPU))

		// Start the workers
		for i := 0; i < int(numCPU); i++ {
			workers[i] = &Worker{histutils.NewHistogram("exponential", 2.0, 1e9)}
			wg.Add(1)
			go worker(i, c, &wg, workers[i].workHistogram)
		}

		// Setup signal catching
		sc := make(chan os.Signal, 1)
		signal.Notify(sc, os.Interrupt, syscall.SIGTERM)

		// Wait for an interrupt
		sig := <-sc

		log.Printf("Received signal: %v\n", sig)

		// Tell work generator to stop, it will also close work channel
		close(done)

		// Wait for all workers to finish
		log.Printf("Waiting for workers to finish\n")
		wg.Wait()

		log.Printf("Writing work histogram\n")
		// write total work histogram to file
		mtx2.Lock()
		totalWorkHistogram.WriteToCSV(workHistogramOutputFilenema)
		mtx2.Unlock()

		log.Printf("Writing CPU Summary worker to finish\n")
		cpuSummaryWg.Wait()

		log.Printf("Writing CPU Summary histogram\n")
		totalCPUHistogram.WriteToCSV(cpuHistogramOutputFilenema)

		log.Printf("Compressing csv files\n")
		files := []string{"total_work_histogram.csv", "total_cpu_histogram.csv", "per_cpu_utilization.csv", "cpu_utilization.csv", "cpu_samples.csv"}
		err := tarballFiles(files, "files.tar.gz")
		if err != nil {
			panic(err)
		}

		// Sleep to allow for CPU profile to be collected
		log.Printf("Sleeping for %v to allow for CPU profile to be collected\n", finalSleepTime)
		time.Sleep(finalSleepTime)
	}
}

func modifyMappedFile(filePath string) {
	const syncEvery = 1000
	const modificationSize = 4096 // Size of each modification

	file, err := os.OpenFile(filePath, os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}

	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileInfo.Size()), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

	rand.Seed(time.Now().UnixNano())

	for i := uint64(0); i < math.MaxUint64; i++ {
		// Random position for modification
		position := rand.Intn(len(data) - modificationSize)
		modification := make([]byte, modificationSize)
		rand.Read(modification)

		// Modify the memory-mapped file
		copy(data[position:], modification)

		if i%syncEvery == 0 {
			// Sync changes to the file
			_, _, errno := syscall.Syscall(syscall.SYS_MSYNC, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), syscall.MS_ASYNC)
			if errno != 0 {
				panic(errno)
			}
		}
	}
}

func createLargeFile(filePath string) {
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	const fileSize = 100 << 30 // 100 GB
	const bufferSize = 1 << 20 // 1 MB buffer size

	buffer := make([]byte, bufferSize)
	rand.Seed(time.Now().UnixNano())

	for written := 0; written < fileSize; written += bufferSize {
		rand.Read(buffer)
		_, err := file.Write(buffer)
		if err != nil {
			panic(err)
		}
	}
}

// // linearRegression function calculates the coefficients of linear regression
// func linearRegression(X, Y []float64) (m, c float64) {
// 	if len(X) != len(Y) {
// 		panic("X and Y should have the same length")
// 	}

// 	var sumX, sumY, sumXY, sumXX float64
// 	N := float64(len(X))

// 	for i := 0; i < len(X); i++ {
// 		sumX += X[i]
// 		sumY += Y[i]
// 		sumXY += X[i] * Y[i]
// 		sumXX += X[i] * X[i]
// 	}

// 	m = (N*sumXY - sumX*sumY) / (N*sumXX - math.Pow(sumX, 2))
// 	c = (sumY - m*sumX) / N

// 	return m, c
// }

// Workers
func worker(i int, c <-chan WorkItem, wg *sync.WaitGroup, h *histutils.Histogram) {
	defer wg.Done()
	var totalWork float64

	for {
		work, ok := <-c
		if !ok {
			// Channel is closed
			log.Printf("Worker %d closing\n", i)
			mtx2.Lock()
			totalWorkHistogram.Add(h)
			mtx2.Unlock()
			return
		}

		mtxStopped.Lock()
		if stopped {
			mtxStopped.Unlock()
			continue
		} else {
			mtxStopped.Unlock()
		}

		// If stale request, skip.
		if work.startTime.After(time.Now()) {
			time.Sleep(time.Until(work.startTime))
		}

		start := time.Now()
		// do some work
		for i := 0; i < int(work.weight); i++ {
			if i%int(workWeight) == 0 {
				mtxStopped.Lock()
				if stopped {
					mtxStopped.Unlock()
					break
				} else {
					mtxStopped.Unlock()
				}
			}
			z := int(workSheet[i%len(workSheet)])
			z = z + 1
			if z%2 == 0 {
				totalWork += float64(z)
			} else {
				totalWork -= float64(z)
			}
		}
		duration := float64(time.Since(start).Microseconds())
		h.RecordValue(duration)
	}
}

// Pareto distribution to simulate heavy tail work
func pareto(alpha, xm float64) float64 {
	u := rand.Float64()
	return (xm / math.Pow(u, 1/alpha)) / paretoAvg
}

// Schedule work on the channel with a Poisson distribution frequency
func scheduleWork(rate float64, c chan WorkItem, done <-chan struct{}) {
	qpns := rate / math.Pow(10, 9)
	var requestTime time.Time = time.Now()

	for {
		var next time.Duration
		var err error
		if arrivalDistribution == "poisson" {
			//poisson arrival rate
			next, err = randutils.GetNextTimePoisson(qpns)
			if err != nil {
				log.Printf("Error generating next time: %v", err)
				continue
			}
		} else {
			//constant arrival rate
			next = time.Duration(1 / qpns)
		}

		requestTime = requestTime.Add(next * time.Nanosecond)
		// After sending all request times generated until GENERATE_REQUESTS_INTERVAL into the future, sleep briefly in the meanwhile to avoid wasting CPU.
		if time.Until(requestTime) > GENERATE_REQUESTS_INTERVAL {
			time.Sleep(GENERATE_REQUESTS_BREAK)
		}

		// mtx2.Lock()
		work := workWeight
		// mtx2.Unlock()

		var workScale float64
		if workDistribution == "pareto" {
			workScale = pareto(alpha, xm)
			// we cap max value so that workers are bounded and eventually finish
			if workScale > 10000 {
				workScale = 10000
			}
		} else {
			workScale = 1
		}
		work = work * workScale

		if work > maxWork {
			work = maxWork
		}

		var parallelism int
		switch workParallelismDistribution {
		case "constant":
			parallelism = maxWorkParallelism
		case "uniform":
			parallelism = rand.Intn(maxWorkParallelism) + 1
		case "exponential":
			// exponential parallelism
			parallelism = int(rand.ExpFloat64()) + 1
			if parallelism > maxWorkParallelism {
				parallelism = maxWorkParallelism
			}
		}

		work = work / float64(parallelism)
		for i := 0; i < parallelism; i++ {
			// push work to channel
			select {
			case <-done:
				log.Println("Work scheduler closing")
				mtxStopped.Lock()
				stopped = true
				mtxStopped.Unlock()
				// If done channel is closed, close work channel and return.
				close(c)
				return
			case c <- WorkItem{requestTime, work}:
				// If channel is not full, push request time.
			default:
				// If channel is full, skip.
			}
		}
	}
}

func updateCPUSummary(done <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()

	// open output file if outputFileName is specified
	var outputFile *os.File
	if cpuSamplesOutputFileName != "" {
		var err error
		outputFile, err = os.Create(cpuSamplesOutputFileName)
		if err != nil {
			log.Fatalf("Error opening output file: %v", err)
		}
		defer outputFile.Close()
		// write header to putput file: "ts,pct,cnt"
		outputFile.WriteString("ts,pct,cnt\n")
	}

	displayTicker := time.NewTicker(time.Duration(displaySeconds) * time.Second)
	var lastCPUUsage int = 0
	var lastTime time.Time = time.Now()
	for {
		select {
		case <-displayTicker.C:
			mtx.Lock()
			// calculate time since last tick
			currentTime := time.Now()
			secondsSinceLastTick := currentTime.Sub(lastTime).Seconds()
			lastTime = currentTime
			currentCPUUsage := latestCPUTime
			// calculate CPU usage since last tick
			cpuUsage := currentCPUUsage - lastCPUUsage
			if lastCPUUsage != 0 {
				totalPct := float64(cpuUsage) / numCPU / secondsSinceLastTick
				// print out avg cpu utilization
				fmt.Printf("Avg CPU utilization: %.2f%%\n", totalPct)

				// // collect avg CPU usage sample for linear regression
				// // add up to 1% of random noise to avoid linear regression from getting stuck
				// X[collectedAvgCPUSamples%regressionHistoryLength] = workWeight + (rand.Float64()-0.5)*0.01*workWeight
				// Y[collectedAvgCPUSamples%regressionHistoryLength] = totalPct
				// collectedAvgCPUSamples++
				// if collectedAvgCPUSamples > 1 {
				// 	// calculate linear regression
				// 	var m, c float64
				// 	if collectedAvgCPUSamples < regressionHistoryLength {
				// 		m, c = linearRegression(X[:collectedAvgCPUSamples], Y[:collectedAvgCPUSamples])
				// 	} else {
				// 		m, c = linearRegression(X, Y)
				// 	}
				// 	fmt.Printf("Regression: y = %.4fx + %.4f\n", m, c)
				// 	// calculate new work weight
				// 	newWorkWeight := (float64(targetCPUPct) - c) / m
				// 	fmt.Printf("New work weight: %.4f\n", newWorkWeight)
				// 	mtx2.Lock()
				// 	workWeight = newWorkWeight
				// 	mtx2.Unlock()
				// }
			}
			emitHistogram(outputFile)
			totalCPUHistogram.Add(histogram)
			// clear histogram
			histogram.ClearCounts()
			mtx.Unlock()
			lastCPUUsage = currentCPUUsage
		case <-done:
			log.Println("CPU summary worker closing")
			return
		}
	}
}

func updateCPUHistogram(done <-chan struct{}) {
	var outputFile *os.File
	if cpuUtilizationOutputFileName != "" {
		var err error
		outputFile, err = os.Create(cpuUtilizationOutputFileName)
		if err != nil {
			log.Fatalf("Error opening output file: %v", err)
		}
		defer outputFile.Close()
		// write header to putput file: "ts,utilization"
		outputFile.WriteString("ts,utilization\n")
	}

	var perCpuOutputFile *os.File
	if perCpuUtilizationOutputFileName != "" {
		var err error
		perCpuOutputFile, err = os.Create(perCpuUtilizationOutputFileName)
		if err != nil {
			log.Fatalf("Error opening output file: %v", err)
		}
		defer perCpuOutputFile.Close()
		// write header to putput file: "ts,cpu,utilization"
		perCpuOutputFile.WriteString("ts,cpu,utilization\n")
	}

	ticker := time.NewTicker(time.Duration(1000/frequency) * time.Millisecond)
	var lastCPUUsage []int = make([]int, int(numCPU)+1)
	lastCPUUsage[0] = 0
	var currentCPUUsage []int = make([]int, int(numCPU)+1)
	var lastTime time.Time = time.Now()

	for {
		select {
		case <-ticker.C:
			// calculate time since last tick
			currentTime := time.Now()
			secondsSinceLastTick := currentTime.Sub(lastTime).Seconds()
			lastTime = currentTime
			getCPUUsage(currentCPUUsage)
			// calculate CPU usage since last tick
			if lastCPUUsage[0] != 0 {
				for i := 0; i < int(numCPU); i++ {
					cpuUsage := currentCPUUsage[i] - lastCPUUsage[i]
					totalPct := float64(cpuUsage) / secondsSinceLastTick
					if i == 0 {
						// at index 0 we have total CPU usage for entire machine
						totalPct = totalPct / numCPU
						if outputFile != nil {
							outputFile.WriteString(fmt.Sprintf("%d,%.2f\n", currentTime.UnixMilli(), totalPct))
						}
						mtx.Lock()
						histogram.RecordValue(totalPct)
						latestCPUTime = currentCPUUsage[0]
						mtx.Unlock()
					} else {
						// at other indexes we have CPU usage for individual CPUs
						if perCpuOutputFile != nil {
							perCpuOutputFile.WriteString(fmt.Sprintf("%d,%d,%.2f\n", currentTime.UnixMilli(), i-1, totalPct))
						}
					}
				}
			}
			for i := 0; i <= int(numCPU); i++ {
				lastCPUUsage[i] = currentCPUUsage[i]
			}
		case <-done:
			log.Println("CPU ticker worker closing")
			return
		}
	}
}

func readProcStat() []string {
	file, err := os.Open("/proc/stat")
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lines []string
	for i := 0; i <= int(numCPU); i++ { // read the first numCPU + 1 lines
		line, _, err := reader.ReadLine()
		if err != nil {
			log.Fatal(err)
		}
		lines = append(lines, string(line))
	}
	return lines
}

func getCPUUsage(usage []int) {
	data := readProcStat()
	for i, line := range data {
		fields := strings.Fields(line)

		user, _ := strconv.Atoi(fields[1])
		nice, _ := strconv.Atoi(fields[2])
		system, _ := strconv.Atoi(fields[3])
		irq, _ := strconv.Atoi(fields[6])
		softirq, _ := strconv.Atoi(fields[7])
		steal, _ := strconv.Atoi(fields[8])

		total := user + nice + system + irq + softirq + steal
		usage[i] = total
	}
}

func emitHistogram(outputFile *os.File) {
	// if outpoutFile is specified, write histogram to file
	if outputFile != nil {
		// write histogram to file
		// for each bucket, write "ts,pct,cnt"
		for _, bucket := range histogram.GetBucketKeys() {
			count, _ := histogram.GetCount(bucket)
			outputFile.WriteString(fmt.Sprintf("%d,%d,%d\n", time.Now().Unix(), int(bucket), count))
		}
	}
	// otherwise, print histogram to stdout
	var output = ""
	for _, bucket := range histogram.GetBucketKeys() {
		if output != "" {
			output += ","
		}
		count, _ := histogram.GetCount(bucket)
		output += fmt.Sprintf("%d", count)
	}
	fmt.Println(output)

}

func tarballFiles(files []string, output string) error {
	outFile, err := os.Create(output)
	if err != nil {
		return err
	}
	defer outFile.Close()

	gzWriter := gzip.NewWriter(outFile)
	defer gzWriter.Close()

	tarWriter := tar.NewWriter(gzWriter)
	defer tarWriter.Close()

	log.Printf("Creating tarball %s", output)
	for _, file := range files {
		log.Printf("Adding %s to tarball", file)
		if err := addFileToTarWriter(file, tarWriter); err != nil {
			return err
		}
	}
	return nil
}

func addFileToTarWriter(filename string, tw *tar.Writer) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	hdr := &tar.Header{
		Name:    filename,
		Mode:    int64(stat.Mode()),
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	_, err = io.Copy(tw, file)
	return err
}
