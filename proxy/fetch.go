package proxy

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
	"users/database"
)

var proxyListSources = []string{
	"https://api.proxyscrape.com/v2/?request=displayproxies&protocol=http&timeout=10000&country=all&ssl=all&anonymity=all",
	"https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
	"https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
	"https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
	"https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
}

// FetchPublicProxies downloads proxy lists from public sources and saves them to DB.
// Returns the number of new proxies added.
func FetchPublicProxies() (int, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	rawCh := make(chan string, 20000)
	var wg sync.WaitGroup

	for _, src := range proxyListSources {
		wg.Add(1)
		go func(url string) {
			defer wg.Done()
			req, err := http.NewRequest(http.MethodGet, url, nil)
			if err != nil {
				return
			}
			req.Header.Set("User-Agent", "Mozilla/5.0")
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				return
			}
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}
			for _, line := range strings.Split(string(body), "\n") {
				line = strings.TrimSpace(line)
				if isValidProxy(line) {
					rawCh <- line
				}
			}
		}(src)
	}

	go func() {
		wg.Wait()
		close(rawCh)
	}()

	seen := map[string]bool{}
	added := 0
	for raw := range rawCh {
		normalized := normalizeProxy(raw)
		if seen[normalized] {
			continue
		}
		seen[normalized] = true
		if _, err := database.SaveProxy(normalized, "http"); err == nil {
			added++
		}
	}

	if added > 0 {
		Default.Reload()
	}
	return added, nil
}

func isValidProxy(s string) bool {
	if s == "" || strings.HasPrefix(s, "#") || strings.HasPrefix(s, "//") {
		return false
	}
	// Strip scheme if present
	stripped := s
	for _, pfx := range []string{"http://", "https://", "socks5://", "socks4://"} {
		stripped = strings.TrimPrefix(stripped, pfx)
	}
	// Must contain host:port
	parts := strings.Split(stripped, ":")
	return len(parts) >= 2 && parts[len(parts)-1] != ""
}

func normalizeProxy(s string) string {
	s = strings.TrimSpace(s)
	lower := strings.ToLower(s)
	if strings.HasPrefix(lower, "http://") || strings.HasPrefix(lower, "https://") ||
		strings.HasPrefix(lower, "socks5://") || strings.HasPrefix(lower, "socks4://") {
		return s
	}
	return "http://" + s
}

// AutoFetchIfEmpty fetches public proxies if the healthy pool is empty.
// Returns a log message describing the action taken.
func AutoFetchIfEmpty() string {
	if Default.Count() > 0 {
		return ""
	}
	count, err := FetchPublicProxies()
	if err != nil {
		return fmt.Sprintf("Auto-fetch failed: %v", err)
	}
	if count == 0 {
		return "Auto-fetch: no new proxies found in public lists"
	}
	return fmt.Sprintf("Auto-fetched %d public proxies (direct connection if all fail)", count)
}
