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

func CheckBlacklist(username string) bool {
	for _, b := range globals.BlackList {
		if strings.EqualFold(b, username) {
			return true
		}
	}
	return false
}

func doRequest(ctx context.Context, username string, p *types.Proxy) (bool, int, error) {
	requestBody := types.UsernameRequest{Username: username}
	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return true, 0, fmt.Errorf("marshal error: %w", err)
	}

	timeout := time.Duration(globals.Config.Timeout) * time.Second
	client, err := proxy.MakeClient(p, timeout)
	if err != nil {
		client = &http.Client{Timeout: timeout}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		globals.DiscordUsernameCheckAPI, bytes.NewBuffer(jsonBody))
	if err != nil {
		return true, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Origin", "https://discord.com")
	req.Header.Set("Referer", "https://discord.com/register")
	req.Header.Set("User-Agent", globals.UserAgent)
	if fingerprint := globals.GetDiscordFingerprint(ctx); fingerprint != "" {
		req.Header.Set("X-Fingerprint", fingerprint)
	}

	if err := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteUsername, globals.Config.MinDelayMs); err != nil {
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
		cooldown := globals.RegisterDiscordRateLimitFor(globals.RateRouteUsername, globals.RetryAfterValue(res.Header, body))
		return true, latency, fmt.Errorf("rate limited (429), cooldown %s", globals.FormatDuration(cooldown))
	}
	globals.ObserveDiscordRateLimitHeadersFor(globals.RateRouteUsername, res.Header)
	if res.StatusCode != http.StatusOK {
		return true, latency, fmt.Errorf("unexpected status: %d", res.StatusCode)
	}

	var r types.UsernameResponse
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return true, latency, fmt.Errorf("decode error: %w", err)
	}

	globals.DecreaseDelay()
	return r.Taken, latency, nil
}

// CheckUsernameSimple does a single check with retry logic
func CheckUsernameSimple(ctx context.Context, username string) (bool, int, error) {
	maxAttempts := globals.Config.Retry.MaxAttempts
	if !globals.Config.Retry.Enabled || maxAttempts < 1 {
		maxAttempts = 1
	}

	pm := proxy.Default
	var p *types.Proxy
	if pm.Count() > 0 {
		p, _ = pm.GetNext()
	}

	rateLimitRetries := 0
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return true, 0, ctx.Err()
		default:
		}

		taken, latency, err := doRequest(ctx, username, p)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return true, latency, err
			}
			if globals.IsRateLimitError(err) && rateLimitRetries < 5 {
				rateLimitRetries++
				if waitErr := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteUsername, globals.Config.MinDelayMs); waitErr != nil {
					return true, latency, waitErr
				}
				attempt--
				continue
			}
			if p != nil {
				pm.MarkFailed(p.ID)
				p, _ = pm.GetNext()
			}
			if attempt < maxAttempts {
				select {
				case <-ctx.Done():
					return true, latency, ctx.Err()
				case <-time.After(time.Duration(400*attempt) * time.Millisecond):
				}
				continue
			}
			return true, latency, err
		}

		if p != nil {
			pm.MarkSuccess(p.ID, latency)
		}
		return taken, latency, nil
	}

	return true, 0, fmt.Errorf("max attempts reached")
}

// CheckUsername does a single check then a second confirmation check if first is available
func CheckUsername(ctx context.Context, username string) (bool, int, error) {
	taken, latency, err := CheckUsernameSimple(ctx, username)
	if err != nil || taken {
		return taken, latency, err
	}

	// Double verify: confirm the username is truly available
	select {
	case <-ctx.Done():
		return true, latency, ctx.Err()
	case <-time.After(300 * time.Millisecond):
	}

	var p2 *types.Proxy
	if proxy.Default.Count() > 0 {
		p2, _ = proxy.Default.GetNext()
	}
	taken2, latency2, err2 := doRequest(ctx, username, p2)
	if err2 != nil {
		return taken, latency, nil
	}
	avgLatency := (latency + latency2) / 2
	return taken2, avgLatency, nil
}
