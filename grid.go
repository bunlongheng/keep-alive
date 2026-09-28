package main

import (
	"bufio"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	slotLen   = 30 * time.Minute
	slotCount = 48 // 24 hours
	iconBase  = "https://bunlongheng.com/app-icons/"
)

type cell struct {
	Status int
	TS     string // local time
	Ms     int64
}

type gridRow struct {
	URL   string
	Host  string
	Icon  string
	Cells []*cell // 1 per slot, nil = no ping in that slot
	row           // all-time numbers for the same URL
}

type grid struct {
	Labels []string // 1 per slot: 12a, 12:30a, 1a ...
	Now    int      // index of the current slot
	Rows   []gridRow
	Day    string // "Sun Sep 27"
	Prev   string // ?day= value of the previous day, "" when the log has none
	Next   string // ?day= value of the next day, "" when Day is today
	Today  bool
}

// buildGrid buckets today's log lines (local midnight to midnight) into 30 minute slots.
// The first ping in a slot is what the cell shows, matching the admin panel.
// buildGrid renders 1 local day; day is any time on that day.
func buildGrid(logPath, iconsPath string, urls []string, day time.Time) grid {
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.Local)
	start := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.Local)
	if start.After(today) {
		start = today
	}
	end := start.Add(slotCount * slotLen)
	g := grid{Day: start.Format("Mon Jan 2"), Now: -1, Today: start.Equal(today)}
	if g.Today {
		g.Now = int(now.Sub(start) / slotLen)
	} else {
		g.Next = start.Add(24 * time.Hour).Format("2006-01-02")
	}
	var first time.Time
	for i := 0; i < slotCount; i++ {
		t := start.Add(time.Duration(i) * slotLen)
		h := t.Hour() % 12
		if h == 0 {
			h = 12
		}
		ap := "a"
		if t.Hour() >= 12 {
			ap = "p"
		}
		if t.Minute() == 0 {
			g.Labels = append(g.Labels, fmt.Sprintf("%d%s", h, ap))
		} else {
			g.Labels = append(g.Labels, fmt.Sprintf("%d:30%s", h, ap))
		}
	}
	idx := map[string]int{}
	for _, u := range urls {
		idx[u] = len(g.Rows)
		g.Rows = append(g.Rows, gridRow{URL: u, Host: host(u), Icon: iconFor(u, iconsPath), Cells: make([]*cell, slotCount)})
	}
	f, err := os.Open(logPath)
	if err != nil {
		return g
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 4 {
			continue
		}
		t, err := time.Parse("2006-01-02 15:04:05", fs[0]+" "+fs[1])
		if err != nil {
			continue
		}
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC).Local()
		if first.IsZero() || t.Before(first) {
			first = t
		}
		if t.Before(start) || !t.Before(end) {
			continue
		}
		i, ok := idx[fs[3]]
		if !ok {
			continue
		}
		slot := int(t.Sub(start) / slotLen)
		if g.Rows[i].Cells[slot] != nil {
			continue
		}
		code, _ := strconv.Atoi(fs[2])
		ms, _ := strconv.ParseInt(strings.TrimSuffix(fs[4:][0], "ms"), 10, 64)
		g.Rows[i].Cells[slot] = &cell{Status: code, TS: t.Format("15:04"), Ms: ms}
	}
	if !first.IsZero() && first.Before(start) {
		g.Prev = start.Add(-24 * time.Hour).Format("2006-01-02")
	}
	return g
}

func host(u string) string {
	if p, err := url.Parse(u); err == nil && p.Host != "" {
		return p.Host
	}
	return u
}

// iconFor maps a URL to its icon: an override from icons.txt, else the host minus the
// Vercel suffix. The page falls back to the site's favicon if the image is missing.
func iconFor(u, iconsPath string) string {
	h := host(u)
	if name, ok := iconOverrides(iconsPath)[h]; ok {
		return iconBase + name + ".png"
	}
	name := strings.TrimSuffix(strings.TrimSuffix(h, ".vercel.app"), "-bheng")
	return iconBase + name + ".png"
}

func iconOverrides(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) == 2 && !strings.HasPrefix(fs[0], "#") {
			out[fs[0]] = fs[1]
		}
	}
	return out
}

// slowMs is the latency at which an alive ping is shown amber.
const slowMs = 2000

type pingRec struct {
	TS     string `json:"ts"` // local "Sep 27 05:30:35"
	Slot   string `json:"slot"`
	Status int    `json:"status"`
	Ms     int64  `json:"ms"`
	OK     bool   `json:"ok"`
	Slow   bool   `json:"slow"` // alive but >= slowMs
}

// pingsFor returns every ping of 1 URL still in the log, oldest first.
func pingsFor(logPath, u string) []pingRec {
	out := []pingRec{}
	f, err := os.Open(logPath)
	if err != nil {
		return out
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 4 || fs[3] != u {
			continue
		}
		t, err := time.Parse("2006-01-02 15:04:05", fs[0]+" "+fs[1])
		if err != nil {
			continue
		}
		t = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC).Local()
		code, _ := strconv.Atoi(fs[2])
		var ms int64
		if len(fs) > 4 {
			ms, _ = strconv.ParseInt(strings.TrimSuffix(fs[4], "ms"), 10, 64)
		}
		slot := t.Truncate(slotLen)
		out = append(out, pingRec{TS: t.Format("Jan 2 15:04:05"), Slot: slot.Format("3:04pm"), Status: code, Ms: ms, OK: alive(code), Slow: alive(code) && ms >= slowMs})
	}
	return out
}
