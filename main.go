// keep-alive pings every URL in a list on a fixed interval so serverless apps stay warm.
// Standard library only. Each round fans out over -workers goroutines, retries network
// errors once, appends "TS  STATUS  URL  MS" lines to a log and writes the latest round
// to status.json. Edit urls.txt at any time; it is re-read every round.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

type result struct {
	URL    string `json:"url"`
	Status int    `json:"status"`
	Ms     int64  `json:"ms"`
	Err    string `json:"err,omitempty"`
}

type round struct {
	TS      string   `json:"ts"`
	Total   int      `json:"total"`
	OK      int      `json:"ok"`
	Failed  int      `json:"failed"`
	Took    string   `json:"took"`
	Results []result `json:"results"`
}

func main() {
	urlsPath := flag.String("urls", "urls.txt", "file with one URL per line, # starts a comment")
	workers := flag.Int("workers", 16, "parallel requests")
	interval := flag.Duration("interval", 5*time.Minute, "time between rounds")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	retries := flag.Int("retries", 1, "retries on network error, never on an HTTP status")
	logPath := flag.String("log", "keep-alive.log", "append log: TS  STATUS  URL  MS")
	statusPath := flag.String("status", "status.json", "latest round as JSON")
	keepLines := flag.Int("keep", 25000, "trim the log to this many lines after each round")
	once := flag.Bool("once", false, "run a single round and exit")
	flag.Parse()

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
			MaxIdleConns:        *workers * 2,
			MaxIdleConnsPerHost: 2,
			IdleConnTimeout:     90 * time.Second,
		},
		// A 301/302 from the app is the app answering; do not chase it.
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}

	for {
		urls, err := readURLs(*urlsPath)
		if err != nil {
			fmt.Fprintln(os.Stderr, "urls:", err)
			os.Exit(1)
		}
		r := runRound(client, urls, *workers, *retries)
		if err := appendLog(*logPath, r, *keepLines); err != nil {
			fmt.Fprintln(os.Stderr, "log:", err)
		}
		if err := writeStatus(*statusPath, r); err != nil {
			fmt.Fprintln(os.Stderr, "status:", err)
		}
		fmt.Printf("%s  %d urls  %d ok  %d failed  %s\n", r.TS, r.Total, r.OK, r.Failed, r.Took)
		for _, x := range r.Results {
			if !alive(x.Status) {
				fmt.Printf("  FAIL %3d  %s  %s\n", x.Status, x.URL, x.Err)
			}
		}
		if *once {
			return
		}
		time.Sleep(*interval)
	}
}

// alive: the app answered. 4xx from its own sign-in page still means the function ran.
// 404 means no deployment, 0 means no answer at all.
func alive(status int) bool { return status > 0 && status < 500 && status != 404 }

func readURLs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	seen := map[string]bool{}
	var urls []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") || seen[line] {
			continue
		}
		seen[line] = true
		urls = append(urls, line)
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("%s has no urls", path)
	}
	return urls, sc.Err()
}

func runRound(client *http.Client, urls []string, workers, retries int) round {
	start := time.Now()
	results := make([]result, len(urls))
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i, u := range urls {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, u string) {
			defer wg.Done()
			defer func() { <-sem }()
			results[i] = ping(client, u, retries)
		}(i, u)
	}
	wg.Wait()
	r := round{
		TS:      start.UTC().Format("2006-01-02 15:04:05"),
		Total:   len(urls),
		Took:    time.Since(start).Round(time.Millisecond).String(),
		Results: results,
	}
	for _, x := range results {
		if alive(x.Status) {
			r.OK++
		} else {
			r.Failed++
		}
	}
	return r
}

func ping(client *http.Client, u string, retries int) result {
	var last result
	for attempt := 0; attempt <= retries; attempt++ {
		t := time.Now()
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return result{URL: u, Err: err.Error()}
		}
		req.Header.Set("User-Agent", "keep-alive/1.0 (+https://github.com/bunlongheng/keep-alive)")
		resp, err := client.Do(req)
		last = result{URL: u, Ms: time.Since(t).Milliseconds()}
		if err != nil {
			last.Err = shortErr(err)
			if attempt < retries {
				time.Sleep(500 * time.Millisecond)
			}
			continue
		}
		// Drain a little so the connection can be reused, then let go.
		io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		last.Status = resp.StatusCode
		return last
	}
	return last
}

// shortErr drops the `Get "https://...": ` prefix Go puts on transport errors.
func shortErr(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return ue.Err.Error()
	}
	return err.Error()
}

func appendLog(path string, r round, keep int) error {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(f)
	for _, x := range r.Results {
		fmt.Fprintf(w, "%s  %d  %s  %dms\n", r.TS, x.Status, x.URL, x.Ms)
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	f.Close()
	return trimLog(path, keep)
}

// trimLog keeps the newest `keep` lines once the file is 20 percent over that.
func trimLog(path string, keep int) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) <= keep+keep/5 {
		return nil
	}
	out := strings.Join(lines[len(lines)-keep:], "\n") + "\n"
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(out), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func writeStatus(path string, r round) error {
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
