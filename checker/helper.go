package checker

import (
        "bytes"
        "context"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "net/http"
        "strings"
        "time"
        "users/globals"
        "users/proxy"
        "users/types"
)

// sharedTransport is a persistent HTTP transport for direct (no-proxy) requests.
// Connection pooling + keep-alive means we reuse TLS sessions instead of
// doing a full TCP+TLS handshake on every single request (~50-100ms savings each).
var sharedTransport = &http.Transport{
        MaxIdleConns:          100,
        MaxIdleConnsPerHost:   20,
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ResponseHeaderTimeout: 15 * time.Second,
        ForceAttemptHTTP2:     true,
        DisableCompression:    false,
}

// sharedClient is used for all direct (no-proxy) requests so connections are reused.
var sharedClient = &http.Client{Transport: sharedTransport}

func CheckBlacklist(username string) bool {
        for _, b := range globals.BlackList {
                if strings.EqualFold(b, username) {
                        return true
                }
        }
        return false
}

// proxyKey returns a stable string key for rate-limit tracking.
// Uses the proxy URL so each distinct IP has its own bucket.
func proxyKey(p *types.Proxy) string {
        if p == nil {
                return globals.ProxyKeyDirect
        }
        return p.URL
}

// tokenRateKey returns a rate-limit bucket key derived from the token.
// Prefix "tok:" separates it from IP-based buckets so it has an independent slot.
func tokenRateKey(token string) string {
        n := 16
        if len(token) < n {
                n = len(token)
        }
        return "tok:" + token[:n]
}

// doRequest performs one username check against Discord.
//
// When a DiscordToken is configured it uses the authenticated endpoint
// (POST /api/v9/unique-username/username-attempt) with Authorization header.
// The rate-limit bucket is then keyed by the token instead of the IP, giving
// an independent 10 req/s slot isolated from other users on the same Replit IP.
//
// Without a token it falls back to the unauthenticated endpoint (per-IP bucket).
func doRequest(ctx context.Context, username string, p *types.Proxy) (bool, int, error) {
        token := globals.Config.DiscordToken
        useToken := token != ""

        // Determine endpoint and rate-limit key
        var endpoint, pk string
        if useToken {
                endpoint = globals.DiscordUsernameCheckAPIAuthed
                pk = tokenRateKey(token)
        } else {
                endpoint = globals.DiscordUsernameCheckAPI
                pk = proxyKey(p)
        }

        requestBody := types.UsernameRequest{Username: username}
        jsonBody, err := json.Marshal(requestBody)
        if err != nil {
                return true, 0, fmt.Errorf("marshal error: %w", err)
        }

        timeout := time.Duration(globals.Config.Timeout) * time.Second
        if timeout <= 0 {
                timeout = 15 * time.Second
        }

        // Choose HTTP client: shared keep-alive client for direct, proxy client otherwise.
        var client *http.Client
        if useToken || p == nil {
                // Use shared persistent client — reuses TLS connections for speed.
                // Timeout is enforced by the request context, not the client.
                client = sharedClient
        } else {
                client, err = proxy.MakeClient(p, timeout)
                if err != nil {
                        client = sharedClient
                }
        }

        req, err := http.NewRequestWithContext(ctx, http.MethodPost,
                endpoint, bytes.NewBuffer(jsonBody))
        if err != nil {
                return true, 0, err
        }

        ua := globals.RandomUserAgent()
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Accept", "application/json")
        req.Header.Set("Accept-Language", "en-US,en;q=0.9")
        req.Header.Set("Accept-Encoding", "gzip, deflate, br")
        req.Header.Set("Origin", "https://discord.com")
        req.Header.Set("Referer", "https://discord.com/register")
        req.Header.Set("User-Agent", ua)
        req.Header.Set("Sec-Fetch-Dest", "empty")
        req.Header.Set("Sec-Fetch-Mode", "cors")
        req.Header.Set("Sec-Fetch-Site", "same-origin")
        req.Header.Set("X-Discord-Locale", "en-US")
        req.Header.Set("X-Discord-Timezone", "America/New_York")

        if useToken {
                req.Header.Set("Authorization", token)
        } else {
                if fingerprint := globals.GetDiscordFingerprint(ctx); fingerprint != "" {
                        req.Header.Set("X-Fingerprint", fingerprint)
                }
        }

        // Wait for a free slot in this bucket (token-based or IP-based)
        if err := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteUsername, pk, globals.Config.MinDelayMs); err != nil {
                return true, 0, err
        }

        start := time.Now()
        res, err := client.Do(req)
        latency := int(time.Since(start).Milliseconds())
        if err != nil {
                return true, latency, err
        }
        defer res.Body.Close()

        if res.StatusCode == 429 {
                body, _ := io.ReadAll(res.Body)
                retryVal := globals.RetryAfterValue(res.Header, body)
                cooldown := globals.RegisterDiscordRateLimitFor(globals.RateRouteUsername, pk, retryVal)
                return true, latency, fmt.Errorf("rate limited (429), cooldown %s", globals.FormatDuration(cooldown))
        }

        if res.StatusCode == 401 && useToken {
                return true, latency, fmt.Errorf("invalid Discord token (401) — check your token in Settings")
        }

        globals.ObserveDiscordRateLimitHeadersFor(globals.RateRouteUsername, pk, res.Header)

        if res.StatusCode == 403 {
                if useToken {
                        return true, latency, fmt.Errorf("token blocked by Discord (403) — account may be flagged")
                }
                return true, latency, fmt.Errorf("IP blocked by Discord (403) — add a Discord token in Settings")
        }
        if res.StatusCode != http.StatusOK {
                return true, latency, fmt.Errorf("unexpected status: %d", res.StatusCode)
        }

        var r types.UsernameResponse
        if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
                return true, latency, fmt.Errorf("decode error: %w", err)
        }

        globals.DecreaseDelayFor(globals.RateRouteUsername, pk)
        return r.Taken, latency, nil
}

// CheckUsernameSimple performs one username check with retry/proxy-rotation logic.
// p is the sticky proxy assigned to this worker (nil = direct connection).
func CheckUsernameSimple(ctx context.Context, username string, p *types.Proxy) (bool, int, error) {
        maxAttempts := globals.Config.Retry.MaxAttempts
        if !globals.Config.Retry.Enabled || maxAttempts < 1 {
                maxAttempts = 1
        }

        pm := proxy.Default
        currentProxy := p // start with the worker's assigned proxy

        rateLimitRetries := 0
        for attempt := 1; attempt <= maxAttempts; attempt++ {
                select {
                case <-ctx.Done():
                        return true, 0, ctx.Err()
                default:
                }

                taken, latency, err := doRequest(ctx, username, currentProxy)
                if err != nil {
                        if errors.Is(err, context.Canceled) {
                                return true, latency, err
                        }

                        if globals.IsRateLimitError(err) && rateLimitRetries < 8 {
                                rateLimitRetries++
                                // Priority 1: rotate within the user-managed proxy pool
                                if pm.Count() > 1 {
                                        if next, e := pm.GetNext(); e == nil && next.URL != proxyKey(currentProxy) {
                                                currentProxy = next
                                        }
                                } else if pm.Count() == 0 && globals.Config.DiscordToken == "" {
                                        // Priority 2: no user proxies, no token — try auto IP rotation
                                        if globals.Config.AutoRotateIP {
                                                proxy.Auto.TriggerFetch() // async, returns immediately
                                                if next, ok := proxy.Auto.GetNext(); ok {
                                                        currentProxy = next
                                                }
                                        }
                                }
                                // Wait for the new bucket's slot
                                if waitErr := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteUsername, proxyKey(currentProxy), globals.Config.MinDelayMs); waitErr != nil {
                                        return true, latency, waitErr
                                }
                                attempt-- // rate-limit retries don't count against maxAttempts
                                continue
                        }

                        // Non-rate-limit error: mark failed, try next proxy
                        if currentProxy != nil {
                                if proxy.IsAutoProxy(currentProxy.ID) {
                                        proxy.Auto.MarkFailed(currentProxy.ID)
                                        if next, ok := proxy.Auto.GetNext(); ok {
                                                currentProxy = next
                                        } else {
                                                currentProxy = nil
                                        }
                                } else {
                                        pm.MarkFailed(currentProxy.ID)
                                        if next, e := pm.GetNext(); e == nil {
                                                currentProxy = next
                                        } else if next, ok := proxy.Auto.GetNext(); ok {
                                                currentProxy = next
                                        } else {
                                                currentProxy = nil
                                        }
                                }
                        }
                        if attempt < maxAttempts {
                                select {
                                case <-ctx.Done():
                                        return true, latency, ctx.Err()
                                case <-time.After(time.Duration(300*attempt) * time.Millisecond):
                                }
                                continue
                        }
                        return true, latency, err
                }

                // Success: update stats for the proxy that did the work
                if currentProxy != nil {
                        if proxy.IsAutoProxy(currentProxy.ID) {
                                // auto-pool proxies have no DB record; no-op
                        } else {
                                pm.MarkSuccess(currentProxy.ID, latency)
                        }
                }
                return taken, latency, nil
        }

        return true, 0, fmt.Errorf("max attempts reached")
}

// CheckUsername checks availability, and if available does a second confirmation
// 300 ms later (double-verify mode) using a different proxy slot if possible.
func CheckUsername(ctx context.Context, username string, p *types.Proxy) (bool, int, error) {
        taken, latency, err := CheckUsernameSimple(ctx, username, p)
        if err != nil || taken {
                return taken, latency, err
        }

        // Double verify: wait briefly then confirm with a second request
        select {
        case <-ctx.Done():
                return true, latency, ctx.Err()
        case <-time.After(300 * time.Millisecond):
        }

        // Try to use a different proxy for the confirmation request
        var p2 *types.Proxy
        pm := proxy.Default
        if pm.Count() > 1 {
                for range 5 {
                        if next, e := pm.GetNext(); e == nil && next.URL != proxyKey(p) {
                                p2 = next
                                break
                        }
                }
        } else if pm.Count() == 1 {
                p2 = p
        }

        taken2, latency2, err2 := doRequest(ctx, username, p2)
        if err2 != nil {
                // First check said available; second errored — trust first
                return taken, latency, nil
        }
        avgLatency := (latency + latency2) / 2
        return taken2, avgLatency, nil
}
