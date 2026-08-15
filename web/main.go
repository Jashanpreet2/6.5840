// mrweb is a small web frontend for the 6.5840 Lab 1 MapReduce
// implementation in ../src/mr. It never imports that code directly;
// instead, for each job it:
//
//  1. saves the uploaded input files into a per-job directory,
//  2. wraps the submitted Map/Reduce functions into a Go plugin and
//     compiles it (from inside the src module, so 6.5840/mr resolves),
//  3. builds and spawns the real mrcoordinator and N mrworker
//     processes, talking over a per-job unix socket,
//  4. merges the resulting mr-out-* files into one sorted output.
package main

import (
	_ "embed"
	"context"
	"flag"
	"fmt"
	"html/template"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

//go:embed index.html
var indexHTML []byte

var (
	port    = flag.Int("port", 8080, "port to listen on")
	srcFlag = flag.String("src", "", "path to the 6.5840 src directory (default: <web dir>/../src)")
	timeout = flag.Duration("timeout", 5*time.Minute, "per-job time limit")
)

var (
	srcMainDir string // src/main inside the lab module; go builds run from here
	jobsDir    string // web/jobs; one subdirectory per job
)

const pluginTemplate = `package main

import (
	"6.5840/mr"
%s)

var _ = mr.KeyValue{}

%s

%s
`

var resultTmpl = template.Must(template.New("result").Parse(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>MapReduce result</title>
<style>
  :root { color-scheme: light dark; }
  body { font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }
  pre { background: #8881; padding: 1rem; border-radius: 6px; overflow-x: auto; font-size: .85rem; }
  .meta { color: #888; }
</style>
</head>
<body>
<h1>{{if .Err}}Job failed{{else}}Job complete{{end}}</h1>
<p class="meta">Job {{.JobID}} &middot; {{.NInputs}} input file(s) &middot; {{.Workers}} worker(s) &middot; {{.Elapsed}}</p>
{{if .Err}}
<p>{{.Err}}</p>
{{if .Detail}}<pre>{{.Detail}}</pre>{{end}}
{{else}}
<p>{{.NLines}} output line(s). <a href="/">Run another job</a></p>
<pre>{{.Output}}</pre>
{{end}}
</body>
</html>
`))

type resultPage struct {
	JobID   string
	NInputs int
	Workers int
	Elapsed string
	Err     string
	Detail  string
	NLines  int
	Output  string
}

func main() {
	flag.Parse()

	webDir, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}
	srcDir := *srcFlag
	if srcDir == "" {
		srcDir = filepath.Join(webDir, "..", "src")
	}
	srcMainDir, err = filepath.Abs(filepath.Join(srcDir, "main"))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(srcMainDir, "mrcoordinator.go")); err != nil {
		log.Fatalf("cannot find mrcoordinator.go in %s (use -src to point at the lab src directory)", srcMainDir)
	}
	jobsDir = filepath.Join(webDir, "jobs")
	if err := os.MkdirAll(jobsDir, 0755); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	http.HandleFunc("/run", handleRun)

	log.Printf("mrweb listening on http://localhost:%d (src: %s)", *port, srcMainDir)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", *port), nil))
}

func handleRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	start := time.Now()
	page := resultPage{JobID: fmt.Sprintf("%d", time.Now().UnixNano())}
	fail := func(msg string, detail string) {
		page.Err = msg
		page.Detail = detail
		page.Elapsed = time.Since(start).Round(time.Millisecond).String()
		w.WriteHeader(http.StatusUnprocessableEntity)
		resultTmpl.Execute(w, page)
	}

	if err := r.ParseMultipartForm(256 << 20); err != nil {
		fail("could not parse form: "+err.Error(), "")
		return
	}

	workers, err := strconv.Atoi(r.FormValue("workers"))
	if err != nil || workers < 1 {
		workers = 1
	}
	if workers > 16 {
		workers = 16
	}
	page.Workers = workers

	fileHeaders := r.MultipartForm.File["inputs"]
	if len(fileHeaders) == 0 {
		fail("no input files uploaded", "")
		return
	}
	page.NInputs = len(fileHeaders)

	jobDir := filepath.Join(jobsDir, "job-"+page.JobID)
	inputDir := filepath.Join(jobDir, "inputs")
	if err := os.MkdirAll(inputDir, 0755); err != nil {
		fail("could not create job directory", err.Error())
		return
	}

	// Save uploaded inputs; each file becomes one map task.
	var inputPaths []string
	for i, fh := range fileHeaders {
		name := filepath.Base(fh.Filename)
		if name == "" || name == "." || name == ".." {
			name = fmt.Sprintf("input-%d", i)
		}
		dst := filepath.Join(inputDir, fmt.Sprintf("%02d-%s", i, name))
		if err := saveUpload(fh, dst); err != nil {
			fail("could not save input file "+name, err.Error())
			return
		}
		inputPaths = append(inputPaths, dst)
	}

	// Generate and compile the application plugin.
	src := fmt.Sprintf(pluginTemplate,
		formatImports(r.FormValue("imports")),
		strings.TrimSpace(r.FormValue("mapf")),
		strings.TrimSpace(r.FormValue("reducef")))
	pluginSrc := filepath.Join(jobDir, "plugin.go")
	pluginSo := filepath.Join(jobDir, "plugin.so")
	if err := os.WriteFile(pluginSrc, []byte(src), 0644); err != nil {
		fail("could not write plugin source", err.Error())
		return
	}
	if out, err := goBuild(jobDir, "-buildmode=plugin", "-o", pluginSo, pluginSrc); err != nil {
		fail("your Map/Reduce code failed to compile", out)
		return
	}

	// Build the coordinator and worker from src/main. Doing this per job
	// keeps the binaries in lockstep with the plugin (Go refuses to load
	// a plugin built against a different version of the mr package).
	binCoord := filepath.Join(jobDir, "mrcoordinator")
	binWorker := filepath.Join(jobDir, "mrworker")
	if out, err := goBuild(jobDir, "-o", binCoord, "mrcoordinator.go"); err != nil {
		fail("failed to build mrcoordinator", out)
		return
	}
	if out, err := goBuild(jobDir, "-o", binWorker, "mrworker.go"); err != nil {
		fail("failed to build mrworker", out)
		return
	}

	output, nLines, errMsg, detail := runJob(jobDir, binCoord, binWorker, pluginSo, inputPaths, workers)
	page.Elapsed = time.Since(start).Round(time.Millisecond).String()
	if errMsg != "" {
		page.Err = errMsg
		page.Detail = detail
		w.WriteHeader(http.StatusInternalServerError)
		resultTmpl.Execute(w, page)
		return
	}
	page.Output = output
	page.NLines = nLines
	os.WriteFile(filepath.Join(jobDir, "output.txt"), []byte(output), 0644)
	resultTmpl.Execute(w, page)
}

// runJob spawns the coordinator and workers, waits for completion, and
// merges the mr-out-* files. Returns (output, lineCount, errMsg, detail).
func runJob(jobDir, binCoord, binWorker, pluginSo string, inputs []string, workers int) (string, int, string, string) {
	sock := filepath.Join(jobDir, "mr.sock")

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	coord := exec.CommandContext(ctx, binCoord, append([]string{sock}, inputs...)...)
	coord.Dir = jobDir
	coordLog, _ := os.Create(filepath.Join(jobDir, "coordinator.log"))
	defer coordLog.Close()
	coord.Stdout = coordLog
	coord.Stderr = coordLog
	if err := coord.Start(); err != nil {
		return "", 0, "failed to start coordinator", err.Error()
	}

	// Give the coordinator a moment to create its socket before the
	// workers try to dial it (they log.Fatal on a failed dial).
	for i := 0; i < 50; i++ {
		if _, err := os.Stat(sock); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	var workerCmds []*exec.Cmd
	for i := 0; i < workers; i++ {
		wc := exec.CommandContext(ctx, binWorker, pluginSo, sock)
		wc.Dir = jobDir
		wlog, _ := os.Create(filepath.Join(jobDir, fmt.Sprintf("worker-%d.log", i)))
		defer wlog.Close()
		wc.Stdout = wlog
		wc.Stderr = wlog
		if err := wc.Start(); err != nil {
			coord.Process.Kill()
			return "", 0, "failed to start worker", err.Error()
		}
		workerCmds = append(workerCmds, wc)
	}

	// The coordinator exits on its own once every reduce task is done.
	coordErr := coord.Wait()

	// Workers exit when told to (or die once the socket goes away);
	// don't let stragglers linger past the job.
	done := make(chan struct{})
	go func() {
		for _, wc := range workerCmds {
			wc.Wait()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		for _, wc := range workerCmds {
			if wc.Process != nil {
				wc.Process.Kill()
			}
		}
		<-done
	}

	if ctx.Err() != nil {
		return "", 0, "job timed out after " + timeout.String(), tailLogs(jobDir)
	}
	if coordErr != nil {
		return "", 0, "coordinator exited with an error", tailLogs(jobDir)
	}

	// Merge mr-out-* into one sorted result.
	matches, err := filepath.Glob(filepath.Join(jobDir, "mr-out-*"))
	if err != nil || len(matches) == 0 {
		return "", 0, "job produced no output files", tailLogs(jobDir)
	}
	var lines []string
	for _, m := range matches {
		data, err := os.ReadFile(m)
		if err != nil {
			return "", 0, "could not read output file " + m, err.Error()
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line != "" {
				lines = append(lines, line)
			}
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n") + "\n", len(lines), "", ""
}

// goBuild runs `go build` from src/main so that imports of 6.5840/mr
// resolve against the lab module, without touching anything in src.
func goBuild(jobDir string, args ...string) (string, error) {
	cmd := exec.Command("go", append([]string{"build"}, args...)...)
	cmd.Dir = srcMainDir
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func formatImports(raw string) string {
	var b strings.Builder
	for _, line := range strings.Split(raw, "\n") {
		imp := strings.Trim(strings.TrimSpace(line), `"`)
		if imp == "" || imp == "6.5840/mr" {
			continue
		}
		fmt.Fprintf(&b, "\t%q\n", imp)
	}
	return b.String()
}

func saveUpload(fh *multipart.FileHeader, dst string) error {
	src, err := fh.Open()
	if err != nil {
		return err
	}
	defer src.Close()
	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()
	_, err = io.Copy(out, src)
	return err
}

// tailLogs returns the last part of each per-job log to help debugging.
func tailLogs(jobDir string) string {
	var b strings.Builder
	logs, _ := filepath.Glob(filepath.Join(jobDir, "*.log"))
	for _, l := range logs {
		data, err := os.ReadFile(l)
		if err != nil {
			continue
		}
		const keep = 2000
		if len(data) > keep {
			data = data[len(data)-keep:]
		}
		fmt.Fprintf(&b, "--- %s ---\n%s\n", filepath.Base(l), data)
	}
	return b.String()
}
