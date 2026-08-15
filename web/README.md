# mrweb — web frontend for the Lab 1 MapReduce implementation

A small Go HTTP server that runs MapReduce jobs on the implementation in
`../src/mr` without modifying anything there.

## Run

```sh
cd web
go run .            # listens on http://localhost:8080
```

Flags: `-port` (default 8080), `-src` (path to the lab `src` directory,
default `../src`), `-timeout` (per-job limit, default 5m).

## Use

Open the page, upload one or more input files (each becomes one map
task), pick the number of workers (1–16), and paste your `Map` and
`Reduce` functions (the form is prefilled with word count). Any standard
library packages your code uses go in the imports box; `6.5840/mr` is
always imported. Submit, and the page returns the merged, sorted output
of all reduce tasks.

## How it works

For each job the server:

1. saves the uploaded files into `web/jobs/job-<id>/inputs/`,
2. wraps the submitted functions into a plugin source file and compiles
   it with `go build -buildmode=plugin` (run from `src/main` so
   `6.5840/mr` resolves against the lab module),
3. builds `mrcoordinator` and `mrworker` into the job directory (per job,
   so the binaries always match the plugin's build of the `mr` package),
4. starts one coordinator and N workers over a per-job unix socket, with
   the job directory as their working directory so `mr-out-*` files land
   there,
5. waits for the coordinator to finish, then merges and sorts all
   `mr-out-*` lines into one result (also saved as `output.txt` in the
   job directory, alongside coordinator/worker logs for debugging).

Notes: the number of reduce tasks is fixed at 10 by
`src/main/mrcoordinator.go`, and map partitions go to the repo's `tmp/`
directory (both paths are baked into the lab code, which this server
deliberately does not touch).
