package proxy

import (
        "fmt"
        "net/http"
        "sync"
        "sync/atomic"
        "time"
        "users/globals"
        "users/types"
)

// AutoPool is an in-memory pool of Discord-tested free proxies.
// Separate from the user-managed DB pool — never persisted.
// Automatically refills itself when triggered by a 429 rate limit.
type AutoPool struct {
        mu        sync.Mutex
        ready     []types.Proxy
        fetching  int32 // atomic: 1 = fetch in progress
        lastFetch time.Time
        nextID    int64 // synthetic negative IDs — no DB collision
}

// Auto is the global auto-rotation pool.
var Auto = &AutoPool{nextID: -10000}

// TriggerFetch kicks off a background fetch+test cycle.
// Returns immediately. Skips if already running or pool is still fresh.
func (ap *AutoPool) TriggerFetch() {
        ap.mu.Lock()
        fresh := time.Since(ap.lastFetch) < 2*time.Minute && len(ap.ready) >= 3
        ap.mu.Unlock()
        if fresh {
                return
        }
        if !atomic.CompareAndSwapInt32(&ap.fetching, 0, 1) {
                return // already in progress
        }
        go ap.run()
}

func (ap *AutoPool) run() {
        defer atomic.StoreInt32(&ap.fetching, 0)

        globals.BroadcastLog("info", "[AutoIP] Rate limited — fetching free proxies to rotate IP...")

        // Pull candidates from all public sources concurrently
        rawCh := make(chan string, 5000)
        var wg sync.WaitGroup
        for _, src := range proxyListSources {
                wg.Add(1)
                go func(u string) {
                        defer wg.Done()
                        lines, err := fetchProxySource(u)
                        if err != nil {
                                return
                        }
                        for _, l := range lines {
                                select {
                                case rawCh <- l:
                                default:
                                }
                        }
                }(src)
        }
        go func() { wg.Wait(); close(rawCh) }()

        // Deduplicate — cap at 300 candidates
        seen := map[string]bool{}
        var candidates []string
        for raw := range rawCh {
                norm := normalizeProxy(raw)
                if !seen[norm] {
                        seen[norm] = true
                        candidates = append(candidates, norm)
                        if len(candidates) >= 300 {
                                break
                        }
                }
        }

        if len(candidates) == 0 {
                globals.BroadcastLog("warn", "[AutoIP] No proxy candidates fetched from public sources")
                ap.mu.Lock()
                ap.lastFetch = time.Now()
                ap.mu.Unlock()
                return
        }

        globals.BroadcastLog("info", fmt.Sprintf("[AutoIP] Testing %d candidates against Discord...", len(candidates)))

        // Test concurrently — stop after 10 working proxies found
        working := discordTestBatch(candidates, 40, 6*time.Second, 10)

        ap.mu.Lock()
        ap.lastFetch = time.Now()
        for i := range working {
                ap.nextID--
                working[i].ID = ap.nextID
        }
        // Prepend fresh proxies, cap pool at 60
        ap.ready = append(working, ap.ready...)
        if len(ap.ready) > 60 {
                ap.ready = ap.ready[:60]
        }
        count := len(ap.ready)
        ap.mu.Unlock()

        if count == 0 {
                globals.BroadcastLog("warn", "[AutoIP] None of the fetched proxies reach Discord — staying on current IP")
        } else {
                globals.BroadcastLog("info", fmt.Sprintf("[AutoIP] %d free proxies ready — IP rotation active", count))
        }
        broadcastPoolCount(count)
}

// GetNext returns the next proxy from the pool (round-robin).
func (ap *AutoPool) GetNext() (*types.Proxy, bool) {
        ap.mu.Lock()
        defer ap.mu.Unlock()
        if len(ap.ready) == 0 {
                return nil, false
        }
        p := ap.ready[0]
        ap.ready = append(ap.ready[1:], p)
        return &p, true
}

// Count returns the number of ready auto proxies.
func (ap *AutoPool) Count() int {
        ap.mu.Lock()
        defer ap.mu.Unlock()
        return len(ap.ready)
}

// MarkFailed removes a proxy from the pool by its synthetic ID.
// Triggers a new fetch if the pool falls below 2.
func (ap *AutoPool) MarkFailed(id int64) {
        ap.mu.Lock()
        for i, p := range ap.ready {
                if p.ID == id {
                        ap.ready = append(ap.ready[:i], ap.ready[i+1:]...)
                        break
                }
        }
        remaining := len(ap.ready)
        low := remaining < 2
        ap.mu.Unlock()
        broadcastPoolCount(remaining)
        if low {
                ap.TriggerFetch()
        }
}

// broadcastPoolCount pushes a real-time pool count update over WebSocket.
func broadcastPoolCount(count int) {
        globals.BroadcastEvent("auto_pool_update", map[string]int{"count": count})
}

// IsAutoProxy returns true when the ID belongs to the auto pool (negative).
func IsAutoProxy(id int64) bool { return id < 0 }

// discordTestBatch tests proxy URL strings against Discord concurrently.
//   - workers: max parallel goroutines
//   - timeout: per-proxy dial+response deadline
//   - want: stop collecting once this many pass (0 = test all)
func discordTestBatch(rawURLs []string, workers int, timeout time.Duration, want int) []types.Proxy {
        jobs := make(chan string, len(rawURLs))
        for _, u := range rawURLs {
                jobs <- u
        }
        close(jobs)

        resultCh := make(chan types.Proxy, len(rawURLs))
        var found int32
        var wg sync.WaitGroup

        for i := 0; i < workers; i++ {
                wg.Add(1)
                go func() {
                        defer wg.Done()
                        for rawURL := range jobs {
                                if want > 0 && int(atomic.LoadInt32(&found)) >= want {
                                        continue // drain without work
                                }
                                p := &types.Proxy{
                                        URL:     rawURL,
                                        Type:    DetectType(rawURL),
                                        Healthy: true,
                                }
                                client, err := MakeClient(p, timeout)
                                if err != nil {
                                        continue
                                }
                                req, err := http.NewRequest(http.MethodGet, "https://discord.com/api/v9/", nil)
                                if err != nil {
                                        continue
                                }
                                req.Header.Set("User-Agent", globals.UserAgent)
                                start := time.Now()
                                resp, err := client.Do(req)
                                if err != nil {
                                        continue
                                }
                                resp.Body.Close()
                                if resp.StatusCode < 500 {
                                        p.AvgLatencyMs = int(time.Since(start).Milliseconds())
                                        atomic.AddInt32(&found, 1)
                                        resultCh <- *p
                                }
                        }
                }()
        }

        go func() { wg.Wait(); close(resultCh) }()

        var out []types.Proxy
        for p := range resultCh {
                out = append(out, p)
                if want > 0 && len(out) >= want {
                        break
                }
        }
        return out
}
