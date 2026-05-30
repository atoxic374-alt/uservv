package proxy

import (
        "context"
        "fmt"
        "math/rand"
        "net"
        "net/http"
        "net/url"
        "strings"
        "sync"
        "time"
        "users/database"
        "users/types"

        goProxy "golang.org/x/net/proxy"
)

type Manager struct {
        mu      sync.RWMutex
        proxies []types.Proxy
}

var Default = &Manager{}

func (m *Manager) Reload() error {
        proxies, err := database.GetHealthyProxies()
        if err != nil {
                return err
        }
        m.mu.Lock()
        m.proxies = proxies
        m.mu.Unlock()
        return nil
}

func (m *Manager) Count() int {
        m.mu.RLock()
        defer m.mu.RUnlock()
        return len(m.proxies)
}

func (m *Manager) GetNext() (*types.Proxy, error) {
        m.mu.RLock()
        defer m.mu.RUnlock()
        if len(m.proxies) == 0 {
                return nil, fmt.Errorf("no healthy proxies")
        }
        p := m.proxies[rand.Intn(len(m.proxies))]
        return &p, nil
}

// All returns a copy of the current proxy list (for sticky worker assignment).
func (m *Manager) All() []types.Proxy {
        m.mu.RLock()
        defer m.mu.RUnlock()
        out := make([]types.Proxy, len(m.proxies))
        copy(out, m.proxies)
        return out
}

func (m *Manager) MarkFailed(id int64) {
        database.UpdateProxyHealth(id, false, 0)
        m.mu.Lock()
        for i, p := range m.proxies {
                if p.ID == id {
                        m.proxies = append(m.proxies[:i], m.proxies[i+1:]...)
                        break
                }
        }
        m.mu.Unlock()
}

func (m *Manager) MarkSuccess(id int64, latencyMs int) {
        database.UpdateProxyHealth(id, true, latencyMs)
}

func DetectType(rawURL string) string {
        lower := strings.ToLower(rawURL)
        if strings.HasPrefix(lower, "socks5://") {
                return "socks5"
        }
        if strings.HasPrefix(lower, "socks4://") {
                return "socks4"
        }
        return "http"
}

func MakeClient(p *types.Proxy, timeout time.Duration) (*http.Client, error) {
        if p == nil {
                return &http.Client{Timeout: timeout}, nil
        }

        proxyType := strings.ToLower(p.Type)

        switch proxyType {
        case "socks5":
                parsedURL, err := url.Parse(p.URL)
                if err != nil {
                        return nil, fmt.Errorf("invalid socks5 url: %w", err)
                }
                var auth *goProxy.Auth
                if parsedURL.User != nil {
                        pass, _ := parsedURL.User.Password()
                        auth = &goProxy.Auth{
                                User:     parsedURL.User.Username(),
                                Password: pass,
                        }
                }
                dialer, err := goProxy.SOCKS5("tcp", parsedURL.Host, auth, goProxy.Direct)
                if err != nil {
                        return nil, fmt.Errorf("socks5 dialer: %w", err)
                }
                transport := &http.Transport{
                        DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
                                return dialer.Dial(network, addr)
                        },
                }
                return &http.Client{Transport: transport, Timeout: timeout}, nil

        default:
                proxyURLStr := p.URL
                if !strings.HasPrefix(strings.ToLower(proxyURLStr), "http") {
                        proxyURLStr = "http://" + proxyURLStr
                }
                parsedURL, err := url.Parse(proxyURLStr)
                if err != nil {
                        return nil, fmt.Errorf("invalid proxy url: %w", err)
                }
                transport := &http.Transport{Proxy: http.ProxyURL(parsedURL)}
                return &http.Client{Transport: transport, Timeout: timeout}, nil
        }
}

func TestProxy(p *types.Proxy, userAgent string) (int, error) {
        client, err := MakeClient(p, 10*time.Second)
        if err != nil {
                return 0, err
        }
        req, err := http.NewRequest(http.MethodGet, "https://discord.com/api/v9/", nil)
        if err != nil {
                return 0, err
        }
        req.Header.Set("User-Agent", userAgent)
        start := time.Now()
        resp, err := client.Do(req)
        if err != nil {
                return 0, err
        }
        defer resp.Body.Close()
        latency := int(time.Since(start).Milliseconds())
        if resp.StatusCode >= 500 {
                return latency, fmt.Errorf("server error: %d", resp.StatusCode)
        }
        return latency, nil
}
