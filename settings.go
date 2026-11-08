package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// settings is what the page can change at runtime: the round interval, the quiet hours
// when no round runs, and which URLs are paused. Persisted to -settings so a restart
// keeps them. Quiet hours are in Eastern time (America/New_York).
type settingsFile struct {
	Interval string   `json:"interval"`
	Quiet    string   `json:"quiet"` // "23-6" = no rounds from 11pm until 6am, "" = off
	Paused   []string `json:"paused"`
}

var (
	settingsPath string
	setMu        sync.RWMutex
	quietStart   = 23
	quietEnd     = 6
	quietOn      = true
	paused       = map[string]bool{}
	wake         = make(chan struct{}, 1)
	eastern      = func() *time.Location {
		if l, err := time.LoadLocation("America/New_York"); err == nil {
			return l
		}
		return time.Local
	}()
)

func loadSettings() {
	b, err := os.ReadFile(settingsPath)
	if err != nil {
		return
	}
	var s settingsFile
	if json.Unmarshal(b, &s) != nil {
		return
	}
	setMu.Lock()
	defer setMu.Unlock()
	if d, err := time.ParseDuration(s.Interval); err == nil && d >= time.Minute {
		interval = d
	}
	if a, b, on, err := parseQuiet(s.Quiet); err == nil {
		quietStart, quietEnd, quietOn = a, b, on
	}
	for _, u := range s.Paused {
		paused[u] = true
	}
}

// parseQuiet reads "23-6"; "" or "off" turns quiet hours off.
func parseQuiet(s string) (start, end int, on bool, err error) {
	s = strings.TrimSpace(strings.ToLower(s))
	if s == "" || s == "off" {
		return 0, 0, false, nil
	}
	if _, err := fmt.Sscanf(s, "%d-%d", &start, &end); err != nil || start < 0 || start > 23 || end < 0 || end > 23 || start == end {
		return 0, 0, false, fmt.Errorf("quiet: use START-END in 24h hours, e.g. 23-6, or off")
	}
	return start, end, true, nil
}

func quietLabel() string {
	setMu.RLock()
	defer setMu.RUnlock()
	if !quietOn {
		return "off"
	}
	return fmt.Sprintf("%d-%d", quietStart, quietEnd)
}

// quietUntil returns when the current quiet window ends, or the zero time when now is
// outside quiet hours.
func quietUntil(now time.Time) time.Time {
	setMu.RLock()
	defer setMu.RUnlock()
	if !quietOn {
		return time.Time{}
	}
	t := now.In(eastern)
	h := t.Hour()
	var in bool
	if quietStart < quietEnd {
		in = h >= quietStart && h < quietEnd
	} else {
		in = h >= quietStart || h < quietEnd
	}
	if !in {
		return time.Time{}
	}
	end := time.Date(t.Year(), t.Month(), t.Day(), quietEnd, 0, 0, 0, eastern)
	if !end.After(t) {
		end = end.Add(24 * time.Hour)
	}
	return end
}

// snapshot builds the file form; call with setMu held.
func snapshot() settingsFile {
	s := settingsFile{Interval: short(interval), Paused: []string{}}
	if quietOn {
		s.Quiet = fmt.Sprintf("%d-%d", quietStart, quietEnd)
	}
	for u := range paused {
		s.Paused = append(s.Paused, u)
	}
	sort.Strings(s.Paused)
	return s
}

// saveSettings writes the file; call with setMu held.
func saveSettings() error { return writeJSON(settingsPath, snapshot()) }

func currentSettings() settingsFile {
	setMu.RLock()
	defer setMu.RUnlock()
	return snapshot()
}

func getInterval() time.Duration {
	setMu.RLock()
	defer setMu.RUnlock()
	return interval
}

func setSchedule(d time.Duration, quiet string) error {
	a, b, on, err := parseQuiet(quiet)
	if err != nil {
		return err
	}
	setMu.Lock()
	defer setMu.Unlock()
	interval, quietStart, quietEnd, quietOn = d, a, b, on
	err = saveSettings()
	select {
	case wake <- struct{}{}:
	default:
	}
	return err
}

func isPaused(u string) bool {
	setMu.RLock()
	defer setMu.RUnlock()
	return paused[u]
}

func setPaused(u string, off bool) error {
	setMu.Lock()
	defer setMu.Unlock()
	if off {
		paused[u] = true
	} else {
		delete(paused, u)
	}
	return saveSettings()
}

func pausedCount() int {
	setMu.RLock()
	defer setMu.RUnlock()
	return len(paused)
}

func activeURLs(urls []string) []string {
	setMu.RLock()
	defer setMu.RUnlock()
	out := urls[:0:0]
	for _, u := range urls {
		if !paused[u] {
			out = append(out, u)
		}
	}
	return out
}

// waitUntil sleeps until t, waking early when settings change so the caller can recompute.
// Returns true when it was woken.
func waitUntil(t time.Time) bool {
	left := time.Until(t)
	if left <= 0 {
		return false
	}
	tm := time.NewTimer(left)
	defer tm.Stop()
	select {
	case <-tm.C:
		return false
	case <-wake:
		return true
	}
}
