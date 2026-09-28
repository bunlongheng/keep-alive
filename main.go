// keep-alive pings every URL in a list on a fixed interval so serverless apps stay warm.
// Standard library only. Each round fans out over -workers goroutines, retries network
// errors once, appends "TS  STATUS  URL  MS" lines to a log, rewrites status.json with the
// latest round and stats.json with all-time totals. Edit urls.txt at any time; it is
// re-read every round. With -listen it also serves a report: text for curl, HTML for a
// browser, /health for monitors. The pinger never waits on the server.
package main

import (
	"bufio"
	"embed"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
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

type urlStat struct {
	Total    int    `json:"total"`
	OK       int    `json:"ok"`
	SumMs    int64  `json:"sumMs"`
	Last     int    `json:"last"`
	LastTS   string `json:"lastTs"`
	LastFail string `json:"lastFail,omitempty"`
}

type stats struct {
	Since     string              `json:"since"`
	Rounds    int                 `json:"rounds"`
	Pings     int                 `json:"pings"`
	OK        int                 `json:"ok"`
	LastRound *round              `json:"lastRound,omitempty"`
	URLs      map[string]*urlStat `json:"urls"`
}

//go:embed static
var static embed.FS

var (
	mu        sync.RWMutex
	allTime   = &stats{URLs: map[string]*urlStat{}}
	interval  time.Duration
	logFile   string
	iconsPath string
	urlsFile  string
)

func main() {
	flag.StringVar(&urlsFile, "urls", "urls.txt", "file with one URL per line, # starts a comment")
	urlsPath := &urlsFile
	workers := flag.Int("workers", 16, "parallel requests")
	flag.DurationVar(&interval, "interval", 5*time.Minute, "time between rounds")
	timeout := flag.Duration("timeout", 10*time.Second, "per-request timeout")
	retries := flag.Int("retries", 1, "retries on network error, never on an HTTP status")
	flag.StringVar(&logFile, "log", "keep-alive.log", "append log: TS  STATUS  URL  MS")
	logPath := &logFile
	flag.StringVar(&iconsPath, "icons", "icons.txt", "host to icon-name overrides for the html page")
	statusPath := flag.String("status", "status.json", "latest round as JSON")
	statsPath := flag.String("stats", "stats.json", "all-time totals as JSON")
	keepLines := flag.Int("keep", 25000, "trim the log to this many lines after each round")
	listen := flag.String("listen", "", "serve the report here, e.g. :8787 (empty = no server)")
	once := flag.Bool("once", false, "run a single round and exit")
	report := flag.Bool("report", false, "print the all-time report from -stats and exit")
	tuiMode := flag.Bool("tui", false, "live terminal dashboard from -stats and -log, refreshes every 5s")
	flag.Parse()

	loadStats(*statsPath)
	if *report {
		fmt.Print(textReport())
		return
	}
	if *tuiMode {
		tui(*statsPath, *logPath, 5*time.Second)
		return
	}
	if *listen != "" {
		go serve(*listen)
	}

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
		record(r)
		if err := appendLog(*logPath, r, *keepLines); err != nil {
			fmt.Fprintln(os.Stderr, "log:", err)
		}
		if err := writeJSON(*statusPath, r); err != nil {
			fmt.Fprintln(os.Stderr, "status:", err)
		}
		mu.RLock()
		err = writeJSON(*statsPath, allTime)
		mu.RUnlock()
		if err != nil {
			fmt.Fprintln(os.Stderr, "stats:", err)
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
		time.Sleep(interval)
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

// record folds a round into the all-time totals.
func record(r round) {
	mu.Lock()
	defer mu.Unlock()
	if allTime.Since == "" {
		allTime.Since = r.TS
	}
	allTime.Rounds++
	rr := r
	allTime.LastRound = &rr
	for _, x := range r.Results {
		s := allTime.URLs[x.URL]
		if s == nil {
			s = &urlStat{}
			allTime.URLs[x.URL] = s
		}
		s.Total++
		s.SumMs += x.Ms
		s.Last = x.Status
		s.LastTS = r.TS
		allTime.Pings++
		if alive(x.Status) {
			s.OK++
			allTime.OK++
		} else {
			s.LastFail = r.TS
		}
	}
}

func loadStats(path string) {
	b, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var s stats
	if json.Unmarshal(b, &s) == nil {
		if s.URLs == nil {
			s.URLs = map[string]*urlStat{}
		}
		mu.Lock()
		allTime = &s
		mu.Unlock()
	}
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

func writeJSON(path string, v interface{}) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ---- report -------------------------------------------------------------------------

type row struct {
	URL   string
	Host  string
	Icon  string
	Pct   float64
	AvgMs int64
	Last  int
	OK    int
	Total int
	Fail  string
}

func rows() ([]row, stats) {
	mu.RLock()
	defer mu.RUnlock()
	var out []row
	for u, s := range allTime.URLs {
		r := row{URL: u, Host: host(u), Icon: iconFor(u, iconsPath), Last: s.Last, OK: s.OK, Total: s.Total, Fail: s.LastFail}
		if s.Total > 0 {
			r.Pct = 100 * float64(s.OK) / float64(s.Total)
			r.AvgMs = s.SumMs / int64(s.Total)
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pct != out[j].Pct {
			return out[i].Pct < out[j].Pct
		}
		return out[i].URL < out[j].URL
	})
	return out, *allTime
}

func pct(ok, total int) float64 {
	if total == 0 {
		return 0
	}
	return 100 * float64(ok) / float64(total)
}

func textReport() string {
	rs, s := rows()
	var b strings.Builder
	fmt.Fprintf(&b, "keep-alive  since %s UTC  rounds %d  pings %d  ok %.2f%%\n", s.Since, s.Rounds, s.Pings, pct(s.OK, s.Pings))
	if s.LastRound != nil {
		fmt.Fprintf(&b, "last round  %s UTC  %d/%d ok  %s\n", s.LastRound.TS, s.LastRound.OK, s.LastRound.Total, s.LastRound.Took)
	}
	fmt.Fprintf(&b, "\n   OK%%     AVG  LAST  URL\n")
	for _, r := range rs {
		fmt.Fprintf(&b, "%6.2f  %5dms  %4d  %s\n", r.Pct, r.AvgMs, r.Last, r.URL)
	}
	return b.String()
}

var page = template.Must(template.New("p").Funcs(template.FuncMap{"aliveInt": alive, "idx": func(i, n int) int { return i - n }}).Parse(`<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>keep-alive</title>
<link rel="icon" type="image/png" href="/favicon.png"><link rel="apple-touch-icon" href="/apple-touch-icon.png">
<link rel="manifest" href="/manifest.webmanifest"><meta name="theme-color" content="#0b0d10">
<meta name="apple-mobile-web-app-capable" content="yes"><meta name="apple-mobile-web-app-status-bar-style" content="black-translucent"><meta name="apple-mobile-web-app-title" content="keep-alive">
<style>
:root{color-scheme:dark}
body{margin:0;background:#0b0d10;color:#c9d1d9;font:13px/1.5 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:28px 20px 60px}
h1{font-size:13px;font-weight:600;letter-spacing:.18em;text-transform:uppercase;color:#7ee787;margin:0 0 4px;display:flex;align-items:center;gap:10px}
h1 img{width:28px;height:28px}
h2{font-size:11px;font-weight:600;letter-spacing:.18em;text-transform:uppercase;color:#8b949e;margin:40px 0 12px}
.sub{color:#8b949e;margin:0 0 24px}
.hd{display:flex;justify-content:space-between;align-items:flex-start;gap:24px;flex-wrap:wrap;margin:0 0 4px}
.hd .sub{margin:0}
.clock{text-align:right;line-height:1;margin-top:-6px;white-space:nowrap;flex:none;width:400px}
.clock .t{font-size:52px;font-weight:700;color:#e6edf3;letter-spacing:-.02em;font-variant-numeric:tabular-nums}
.clock .t small{font-size:20px;font-weight:500;color:#8b949e;margin-left:6px}
.clock .date{font-size:16px;color:#7ee787;letter-spacing:.12em;text-transform:uppercase;margin-top:10px}
.clock .date.past{color:#d29922;text-shadow:0 0 12px rgba(210,153,34,.45)}
.clock .nx{font-size:12px;color:#6e7681;margin-top:10px;letter-spacing:.06em}.clock .nx b{color:#58a6ff;font-weight:600;font-variant-numeric:tabular-nums;text-shadow:0 0 12px rgba(88,166,255,.6)}
.grid th.t.now{color:#7ee787;text-shadow:0 0 10px rgba(63,185,80,.8)}
.grid td.c.now{background:linear-gradient(180deg,rgba(63,185,80,.14),rgba(63,185,80,.06));box-shadow:inset 0 0 14px rgba(63,185,80,.18)}
.grid tr:first-child th.t.now{background:linear-gradient(0deg,rgba(63,185,80,.14),transparent)}
.d.now{box-shadow:0 0 0 3px rgba(63,185,80,.3),0 0 12px rgba(63,185,80,.9)}
@keyframes pulse{0%,100%{box-shadow:0 0 0 3px rgba(63,185,80,.3),0 0 10px rgba(63,185,80,.7)}50%{box-shadow:0 0 0 4px rgba(63,185,80,.15),0 0 18px rgba(63,185,80,1)}}
.d.now.ok{animation:pulse 2.4s ease-in-out infinite}
.sub b{color:#c9d1d9;font-weight:600}
table{border-collapse:collapse;width:100%}
th{text-align:left;color:#8b949e;font-weight:500;padding:0 12px 8px 0;border-bottom:1px solid #21262d}
td{padding:6px 12px 6px 0;border-bottom:1px solid #161b22;white-space:nowrap}
td.n{text-align:right;font-variant-numeric:tabular-nums}
.app{display:flex;align-items:center;gap:8px}
.app img{width:16px;height:16px;border-radius:4px;flex:none;background:#21262d}
.dn{width:36px;height:36px;display:block;transform:rotate(-90deg)}
.dn circle{fill:none;stroke-width:4}
.dn .t{stroke:#21262d}
.dn .v{stroke:#3fb950;stroke-linecap:round;transition:stroke-dasharray .6s}
.dn.warn .v{stroke:#d29922}.dn.bad .v{stroke:#f85149}
.dn text{transform:rotate(90deg);transform-origin:18px 18px;fill:#c9d1d9;font:700 8px/1 ui-monospace,SFMono-Regular,Menlo,monospace;text-anchor:middle;letter-spacing:-.2px}
.grid td.u{padding:5px 12px 5px 0}
.sub b.bad{color:#f85149}
a{color:#c9d1d9;text-decoration:none}a:hover{color:#fff}
.fail{color:#f85149}
.days{display:flex;align-items:center;gap:10px;margin:6px 0 8px}
.days a{color:#c9d1d9;font-size:22px;line-height:1;padding:2px 9px 5px;border:1px solid #30363d;border-radius:6px;background:#161b22}.days a:hover{border-color:#8b949e}
.days a.off{opacity:.25;pointer-events:none}.days .lbl{width:160px;display:flex;align-items:center;justify-content:center;gap:8px}.days b{color:#e6edf3;font-weight:600;font-size:14px}.days i{font-style:normal;color:#3fb950;font-size:11px;letter-spacing:.08em;text-transform:uppercase}
.wrap{overflow-x:clip}
.days,.wrap,.clock{transition:opacity .18s ease}.swap .days,.swap .wrap,.swap .clock .date,.swap .clock .nx{opacity:0}
@media (max-width:1380px){.grid th.t,.grid td.c{display:none}}
.grid{border-collapse:separate;border-spacing:0;width:100%;max-width:none}
.grid th{border:0;padding:0 0 6px}
.grid th.t{writing-mode:vertical-rl;transform:rotate(180deg);font-size:9px;color:#6e7681;padding:0 0 0 2px;height:40px;text-align:left;vertical-align:top}
.grid td{padding:4px 0;border-bottom:1px solid #161b22}
.grid td.name,.grid th.name{width:1%;padding-right:14px;white-space:nowrap;position:sticky;left:0;background:#0b0d10;text-align:left}
.grid th{color:#8b949e;font-weight:500;padding-right:12px;vertical-align:bottom}
.grid td.u,.grid td.n,.grid th.n{width:1%;padding-right:12px;white-space:nowrap;vertical-align:middle}.grid td.n{text-align:right}
.grid td.c:first-of-type{padding-left:8px}
.d{display:block;width:9px;height:9px;border-radius:50%;margin:0 auto;border:1px solid #30363d;box-sizing:border-box}
.grid td.c{min-width:19px;text-align:center}
.d.ok{background:#3fb950;border-color:#3fb950}
.d.ko{background:#f85149;border-color:#f85149}
.d.now{box-shadow:0 0 0 3px rgba(63,185,80,.25)}
.d.now.ko{box-shadow:0 0 0 3px rgba(248,81,73,.25)}
@keyframes blink{0%,100%{opacity:.2;transform:scale(.8)}50%{opacity:1;transform:scale(1.1)}}
.d.now.miss{background:#3fb950;border-color:#3fb950;animation:blink 1s ease-in-out infinite}
@keyframes colpulse{0%,100%{box-shadow:inset 0 0 14px rgba(63,185,80,.12)}50%{box-shadow:inset 0 0 22px rgba(63,185,80,.4)}}
.grid td.c.now:has(.miss){animation:colpulse 1s ease-in-out infinite}
.grid tr.r{cursor:pointer}.grid tr.r:hover td{background:#0f1319}.grid tr.r.open td{background:#0f1319}
.grid tr.r.open td.name{background:#0f1319}
.det td{padding:0;background:#0d1015;border-bottom:1px solid #21262d}
.det .in{padding:14px 16px 16px;position:sticky;left:0;max-width:calc(100vw - 40px);box-sizing:border-box}
.det .sum{color:#8b949e;margin-bottom:12px;font-size:13px}.det .sum b{color:#c9d1d9;font-weight:600}
.det .sum b.ko{color:#f85149}.det .sum b.warn{color:#d29922}
.pt{max-height:420px;overflow-y:auto;max-width:560px;border:1px solid #21262d;border-radius:6px}
.pt table{width:100%;border-collapse:collapse;font-size:13px;font-variant-numeric:tabular-nums}
.pt th{position:sticky;top:0;background:#161b22;color:#8b949e;font-weight:500;text-align:left;padding:7px 12px;border-bottom:1px solid #30363d}
.pt td{padding:6px 12px;border-bottom:1px solid #161b22;white-space:nowrap;background:transparent}
.pt td.ts{color:#e6edf3;font-weight:600}.pt td.sl{color:#6e7681}.pt td.st{color:#7ee787}.pt td.ms{text-align:right}
.pt tr.ko td.st,.pt tr.ko td.ts{color:#f85149}.pt tr.ko .lb{background:#f85149}
.pt tr.warn td.st,.pt tr.warn td.ts{color:#d29922}.pt tr.warn .lb{background:#d29922}
.pt .lb{display:inline-block;height:6px;background:#3fb950;border-radius:3px;margin-right:8px;vertical-align:middle}
.pt tr.ko .lb{background:#f85149}
</style></head><body>
<div class="hd"><div><h1><img src="/icon.png" alt="">keep-alive</h1>
<p class="sub"><b>{{len .G.Rows}}</b> apps, all pinged every <b>{{.Every}}</b> · <b>{{.S.Rounds}}</b> rounds since <b>{{.S.Since}}</b> UTC = <b>{{.S.Pings}}</b> pings, <b>{{printf "%.2f" .Pct}}%</b> ok{{if .S.LastRound}} · last round: {{if .Down}}<b class="bad">{{.Down}} down</b>{{else}}<b>all up</b>{{end}} in {{.S.LastRound.Took}}{{end}}</p></div>
<div class="clock"><div class="t" id="ct"></div>{{if .G.Today}}<div class="date" id="cd"></div><div class="nx">next run <b id="nx" data-at="{{.Next}}"></b></div>{{else}}<div class="date past">{{.G.Long}}</div><div class="nx">&nbsp;</div>{{end}}</div></div>
<div class="days"><a id="dp" href="?day={{.G.Prev}}"{{if not .G.Prev}} class="off"{{end}}>&#8249;</a><span class="lbl"><b>{{.G.Day}}</b>{{if .G.Today}}<i>today</i>{{end}}</span><a id="dn" href="?day={{.G.Next}}"{{if not .G.Next}} class="off"{{end}}>&#8250;</a></div>
<div class="wrap"><table class="grid">
<tr><th class="name">app</th><th>uptime</th><th class="n">avg</th><th class="n">last</th>{{$now := .G.Now}}{{range $i, $l := .G.Labels}}<th class="t{{if eq $i $now}} now{{end}}">{{$l}}</th>{{end}}</tr>
{{range .G.Rows}}<tr class="r" data-url="{{.URL}}"><td class="name"><span class="app"><img src="{{.Icon}}" alt="" loading="lazy" onerror="if(!this.dataset.f){this.dataset.f=1;this.src='https://icons.duckduckgo.com/ip3/{{.Host}}.ico'}"><a href="{{.URL}}" target="_blank" rel="noreferrer">{{.Host}}</a></span></td>
<td class="u"><svg class="dn{{if lt .Pct 99.0}} bad{{else if lt .Pct 100.0}} warn{{end}}" viewBox="0 0 36 36"><circle class="t" cx="18" cy="18" r="15"/><circle class="v" cx="18" cy="18" r="15" pathLength="100" stroke-dasharray="{{printf "%.2f" .Pct}} 100"/><text x="18" y="21">{{if ge .Pct 100.0}}100{{else}}{{printf "%.1f" .Pct}}{{end}}%</text></svg></td>
<td class="n">{{.AvgMs}}ms</td><td class="n{{if not (aliveInt .Last)}} fail{{end}}">{{.Last}}</td>
{{$now := $.G.Now}}{{range $i, $c := .Cells}}<td class="c{{if eq $i $now}} now{{end}}"><span class="d{{if $c}}{{if aliveInt $c.Status}} ok{{else}} ko{{end}}{{else if eq $i $now}} miss{{end}}{{if eq $i $now}} now{{end}}"{{if $c}} title="{{$c.TS}}  {{$c.Status}}  {{$c.Ms}}ms"{{end}}></span></td>{{end}}
</tr>{{end}}
</table></div>
<script>
(function(){var D=["Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"],M=["January","February","March","April","May","June","July","August","September","October","November","December"];
function tick(){var n=new Date(),h=n.getHours(),p=function(x){return String(x).padStart(2,"0")},ct=document.getElementById("ct"),cd=document.getElementById("cd");
ct.innerHTML=(h%12||12)+":"+p(n.getMinutes())+":"+p(n.getSeconds())+"<small>"+(h<12?"AM":"PM")+"</small>";
if(!cd)return;cd.textContent=D[n.getDay()]+" · "+M[n.getMonth()]+" "+n.getDate()+", "+n.getFullYear();
var nx=document.getElementById("nx"),at=+nx.dataset.at,left=Math.round((at-n)/1000);
nx.textContent=!at?"?":left<=0?"running":Math.floor(left/60)+":"+p(left%60)}
window.tick=tick;tick();setInterval(tick,1000)})();
async function openRow(tr){
  if(tr.nextElementSibling&&tr.nextElementSibling.classList.contains("det"))return;
  tr.classList.add("open");
  var det=document.createElement("tr");det.className="det";
  det.innerHTML='<td colspan="'+tr.children.length+'"><div class="in"><div class="sum">loading</div></div></td>';
  tr.after(det);
  var r=await fetch("/pings?url="+encodeURIComponent(tr.dataset.url));var d=await r.json();
  var n=d.pings.length,fails=d.pings.filter(function(p){return !p.ok}),slow=d.pings.filter(function(p){return p.slow}),good=n-fails.length-slow.length;
  var ms=d.pings.map(function(p){return p.ms});
  var avg=n?Math.round(ms.reduce(function(a,b){return a+b},0)/n):0;
  var h='<div class="sum"><b>'+n+'</b> pings'+(n?' since <b>'+d.pings[0].ts+'</b>':'')+' · <b class="'+(fails.length?'ko':'')+'">'+fails.length+' failed</b> · <b class="'+(slow.length?'warn':'')+'">'+slow.length+' slow</b> · <b>'+good+' ok</b> · avg <b>'+avg+'ms</b></div>';
  var mx=n?Math.max.apply(null,ms):1;
  var rank=function(p){return p.ok?(p.slow?1:2):0};
  var rows=d.pings.slice().reverse().sort(function(a,b){return rank(a)-rank(b)});
  h+='<div class="pt"><table><tr><th>time</th><th>slot</th><th>status</th><th style="text-align:right">latency</th></tr>'
    +rows.map(function(p){
      return '<tr class="'+(p.ok?(p.slow?'warn':''):'ko')+'"><td class="ts">'+p.ts+'</td><td class="sl">'+p.slot+'</td><td class="st">'+p.status+'</td><td class="ms"><span class="lb" style="width:'+Math.max(4,Math.round(90*p.ms/mx))+'px"></span>'+p.ms+'ms</td></tr>'}).join("")
    +'</table></div>';
  det.querySelector(".in").innerHTML=h;
}
var swapping=false;
async function goDay(href,silent){
  if(swapping)return;swapping=true;
  var open=[].map.call(document.querySelectorAll("tr.r.open"),function(t){return t.dataset.url});
  if(!silent){document.body.classList.add("swap");var wait=new Promise(function(r){setTimeout(r,180)})}
  try{
    var h=await (await fetch(href,{headers:{Accept:"text/html"}})).text();
    if(!silent)await wait;
    var d=new DOMParser().parseFromString(h,"text/html");
    [".clock",".days",".wrap"].forEach(function(s){document.querySelector(s).replaceWith(d.querySelector(s))});
    tick();if(!silent)history.pushState(null,"",href);
    open.forEach(function(u){var tr=document.querySelector('tr.r[data-url="'+u+'"]');if(tr)openRow(tr)});
  }finally{
    void document.body.offsetWidth;document.body.classList.remove("swap");swapping=false;
  }
}
function arrow(id){var a=document.getElementById(id);if(a&&!a.classList.contains("off"))goDay(a.getAttribute("href"))}
document.addEventListener("keydown",function(e){if(e.key==="ArrowLeft")arrow("dp");else if(e.key==="ArrowRight")arrow("dn")});
document.addEventListener("click",function(e){
  var a=e.target.closest("#dp,#dn");if(a){e.preventDefault();arrow(a.id);return}
  var tr=e.target.closest("tr.r");if(!tr||e.target.closest("a"))return;
  tr.classList.contains("open")?closeRow(tr):openRow(tr)});
window.addEventListener("popstate",function(){goDay(location.href,true)});
setInterval(function(){goDay(location.href,true)},60000);
function closeRow(tr){var n=tr.nextElementSibling;if(n&&n.classList.contains("det"))n.remove();tr.classList.remove("open")}
</script>
</body></html>`))

func serve(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/" {
			http.NotFound(w, req)
			return
		}
		if !strings.Contains(req.Header.Get("Accept"), "text/html") {
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			io.WriteString(w, textReport())
			return
		}
		rs, s := rows()
		urls, _ := readURLs(urlsFile) // rows follow urls.txt order
		day := time.Now()
		if d, err := time.ParseInLocation("2006-01-02", req.URL.Query().Get("day"), time.Local); err == nil {
			day = d
		}
		g := buildGrid(logFile, iconsPath, urls, day)
		byURL := map[string]row{}
		for _, r := range rs {
			byURL[r.URL] = r
		}
		for i := range g.Rows {
			g.Rows[i].row = byURL[g.Rows[i].URL]
		}
		var next int64
		if s.LastRound != nil {
			if t, err := time.Parse("2006-01-02 15:04:05", s.LastRound.TS); err == nil {
				next = t.Add(interval).UnixMilli()
			}
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		down := 0
		if s.LastRound != nil {
			down = s.LastRound.Total - s.LastRound.OK
		}
		page.Execute(w, map[string]interface{}{"S": s, "Down": down, "Pct": pct(s.OK, s.Pings), "G": g, "Next": next, "Every": short(interval)})
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, req *http.Request) {
		mu.RLock()
		lr := allTime.LastRound
		mu.RUnlock()
		if lr == nil {
			http.Error(w, "no round yet", http.StatusServiceUnavailable)
			return
		}
		t, _ := time.Parse("2006-01-02 15:04:05", lr.TS)
		if time.Since(t) > 3*interval {
			http.Error(w, "stale: last round "+lr.TS, http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintf(w, "ok %d/%d at %s\n", lr.OK, lr.Total, lr.TS)
	})
	for _, f := range []string{"icon.png", "apple-touch-icon.png", "favicon.png", "manifest.webmanifest"} {
		name := f
		mux.HandleFunc("/"+name, func(w http.ResponseWriter, req *http.Request) {
			b, _ := static.ReadFile("static/" + name)
			if strings.HasSuffix(name, ".png") {
				w.Header().Set("Content-Type", "image/png")
			} else {
				w.Header().Set("Content-Type", "application/manifest+json")
			}
			w.Header().Set("Cache-Control", "public, max-age=86400")
			w.Write(b)
		})
	}
	mux.HandleFunc("/pings", func(w http.ResponseWriter, req *http.Request) {
		u := req.URL.Query().Get("url")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{"url": u, "pings": pingsFor(logFile, u)})
	})
	mux.HandleFunc("/stats.json", func(w http.ResponseWriter, req *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(allTime)
	})
	mux.HandleFunc("/status.json", func(w http.ResponseWriter, req *http.Request) {
		mu.RLock()
		defer mu.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(allTime.LastRound)
	})
	if err := http.ListenAndServe(addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, "serve:", err)
	}
}

// short prints 30m, 2h, 90s instead of 30m0s.
func short(d time.Duration) string {
	switch {
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	case d%time.Minute == 0:
		return fmt.Sprintf("%dm", int(d/time.Minute))
	}
	return d.String()
}
