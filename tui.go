package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// tui redraws an all-time report in the terminal every few seconds from the files the
// pinger writes, so it works over ssh with no server and no extra dependency.
func tui(statsPath, logPath string, every time.Duration) {
	fmt.Print("\x1b[?25l\x1b[?1049h") // hide cursor, alternate screen
	restore := func() { fmt.Print("\x1b[?1049l\x1b[?25h") }
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		loadStats(statsPath)
		fmt.Print("\x1b[H\x1b[2J" + tuiFrame(recentByURL(logPath, 40), termWidth()))
		select {
		case <-sig:
			restore()
			return
		case <-tick.C:
		}
	}
}

const (
	cDim   = "\x1b[2m"
	cGreen = "\x1b[32m"
	cRed   = "\x1b[31m"
	cAmber = "\x1b[33m"
	cBold  = "\x1b[1m"
	cOff   = "\x1b[0m"
)

func tuiFrame(recent map[string][]int, width int) string {
	rs, s := rows()
	var b strings.Builder
	fmt.Fprintf(&b, "%skeep-alive%s  %ssince %s UTC  rounds %d  pings %d%s  ", cBold, cOff, cDim, s.Since, s.Rounds, s.Pings, cOff)
	p := pct(s.OK, s.Pings)
	fmt.Fprintf(&b, "%s%.2f%% ok%s\n", tone(p), p, cOff)
	if s.LastRound != nil {
		age := "?"
		if t, err := time.Parse("2006-01-02 15:04:05", s.LastRound.TS); err == nil {
			age = time.Since(t).Round(time.Second).String()
		}
		c := cGreen
		if s.LastRound.Failed > 0 {
			c = cRed
		}
		fmt.Fprintf(&b, "%slast round %s ago%s  %s%d/%d ok%s in %s\n", cDim, age, cOff, c, s.LastRound.OK, s.LastRound.Total, cOff, s.LastRound.Took)
	}
	urlW := width - 8 - 8 - 6 - 42 - 6
	if urlW < 20 {
		urlW = 20
	}
	fmt.Fprintf(&b, "\n%s%7s  %6s  %4s  %-40s  %s%s\n", cDim, "OK%", "AVG", "LAST", "LAST 40 ROUNDS", "APP", cOff)
	for _, r := range rs {
		u := strings.TrimPrefix(strings.TrimPrefix(r.URL, "https://"), "http://")
		if len(u) > urlW {
			u = u[:urlW-1] + "…"
		}
		last := cGreen
		if !alive(r.Last) {
			last = cRed
		}
		fmt.Fprintf(&b, "%s%6.2f%%%s  %5dms  %s%4d%s  %-40s  %s\n", tone(r.Pct), r.Pct, cOff, r.AvgMs, last, r.Last, cOff, spark(recent[r.URL], 40), u)
	}
	fmt.Fprintf(&b, "\n%sctrl-c to quit%s", cDim, cOff)
	return b.String()
}

func tone(p float64) string {
	switch {
	case p >= 100:
		return cGreen
	case p >= 99:
		return cAmber
	}
	return cRed
}

// spark draws 1 block per round, newest on the right, padded on the left.
func spark(st []int, n int) string {
	var b strings.Builder
	for i := 0; i < n-len(st); i++ {
		b.WriteString(cDim + "·" + cOff)
	}
	for _, s := range st {
		if alive(s) {
			b.WriteString(cGreen + "▮" + cOff)
		} else {
			b.WriteString(cRed + "▮" + cOff)
		}
	}
	return b.String()
}

// recentByURL reads the log tail and returns the last n statuses per URL, oldest first.
func recentByURL(path string, n int) map[string][]int {
	out := map[string][]int{}
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	if st, err := f.Stat(); err == nil && st.Size() > 512<<10 {
		f.Seek(st.Size()-512<<10, 0)
	}
	sc := bufio.NewScanner(f)
	sc.Scan() // drop a possibly partial first line
	for sc.Scan() {
		fs := strings.Fields(sc.Text())
		if len(fs) < 4 {
			continue
		}
		code, err := strconv.Atoi(fs[2])
		if err != nil {
			continue
		}
		out[fs[3]] = append(out[fs[3]], code)
	}
	for u, st := range out {
		if len(st) > n {
			out[u] = st[len(st)-n:]
		}
	}
	return out
}

func termWidth() int {
	cmd := exec.Command("stty", "size")
	cmd.Stdin = os.Stdin
	if b, err := cmd.Output(); err == nil {
		if fs := strings.Fields(string(b)); len(fs) == 2 {
			if w, err := strconv.Atoi(fs[1]); err == nil && w > 0 {
				return w
			}
		}
	}
	return 120
}
