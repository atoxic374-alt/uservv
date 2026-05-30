package vanity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"users/database"
	"users/globals"
	"users/proxy"
	"users/types"
)

const DiscordInviteAPI = "https://discord.com/api/v9/invites/%s?with_counts=false"

const (
	vanityStateIdle int32 = iota
	vanityStateRunning
	vanityStateStopping
)

var (
	Available  int64
	Taken      int64
	Errors     int64
	VRunning   int32
	VCancel    context.CancelFunc
	VStartTime time.Time
	VSessionID int64
	VTotal     int64

	vanityErrorLogCount int64
)

func IsRunning() bool { return atomic.LoadInt32(&VRunning) != vanityStateIdle }
func Status() string {
	switch atomic.LoadInt32(&VRunning) {
	case vanityStateRunning:
		return "running"
	case vanityStateStopping:
		return "stopping"
	default:
		return "idle"
	}
}
func TryStart() bool {
	return atomic.CompareAndSwapInt32(&VRunning, vanityStateIdle, vanityStateRunning)
}
func RequestStop() bool {
	for {
		state := atomic.LoadInt32(&VRunning)
		if state == vanityStateIdle {
			return false
		}
		if state == vanityStateStopping {
			return true
		}
		if atomic.CompareAndSwapInt32(&VRunning, state, vanityStateStopping) {
			return true
		}
	}
}
func SetStopped() { atomic.StoreInt32(&VRunning, vanityStateIdle) }
func SetRunning(v bool) {
	if v {
		atomic.StoreInt32(&VRunning, vanityStateRunning)
	} else {
		SetStopped()
	}
}
func GetRate() float64 {
	if !IsRunning() {
		return 0
	}
	elapsed := time.Since(VStartTime).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(atomic.LoadInt64(&Available)+atomic.LoadInt64(&Taken)+atomic.LoadInt64(&Errors)) / elapsed
}

func logVanityCheckError(code string, err error, workerID int) {
	compact := globals.CompactCheckError(err)
	total, ok := globals.ShouldBroadcastCheckError(&vanityErrorLogCount)
	if !ok {
		return
	}
	msg := fmt.Sprintf("[Vanity] Check error [%s]: %s [T%d] (%d total)", code, compact, workerID, total)
	globals.BroadcastLog("warn", msg)
}

// CheckCode checks a single vanity code. Returns taken, guildName, latency, error.
func CheckCode(ctx context.Context, code string) (bool, string, int, error) {
	var p *types.Proxy
	pm := proxy.Default
	if pm.Count() > 0 {
		p, _ = pm.GetNext()
	}

	timeout := time.Duration(globals.VanityConfig.Timeout) * time.Second
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client, err := proxy.MakeClient(p, timeout)
	if err != nil {
		client = &http.Client{Timeout: timeout}
	}

	url := fmt.Sprintf(DiscordInviteAPI, code)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, "", 0, err
	}
	req.Header.Set("User-Agent", globals.UserAgent)
	req.Header.Set("Accept", "application/json")

	if err := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteVanity, globals.VanityConfig.MinDelayMs); err != nil {
		return false, "", 0, err
	}

	start := time.Now()
	res, err := client.Do(req)
	latency := int(time.Since(start).Milliseconds())
	if err != nil {
		if p != nil {
			pm.MarkFailed(p.ID)
		}
		return false, "", latency, err
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	if p != nil {
		pm.MarkSuccess(p.ID, latency)
	}

	switch res.StatusCode {
	case 404:
		globals.ObserveDiscordRateLimitHeadersFor(globals.RateRouteVanity, res.Header)
		globals.DecreaseDelayFor(globals.RateRouteVanity)
		return false, "", latency, nil
	case 200:
		globals.ObserveDiscordRateLimitHeadersFor(globals.RateRouteVanity, res.Header)
		globals.DecreaseDelayFor(globals.RateRouteVanity)
		return true, extractGuildName(string(body)), latency, nil
	case 429:
		cooldown := globals.RegisterDiscordRateLimitFor(globals.RateRouteVanity, globals.RetryAfterValue(res.Header, body))
		return false, "", latency, fmt.Errorf("rate limited (429), cooldown %s", globals.FormatDuration(cooldown))
	default:
		return false, "", latency, fmt.Errorf("status %d", res.StatusCode)
	}
}

func extractGuildName(body string) string {
	var payload struct {
		Guild struct {
			Name string `json:"name"`
		} `json:"guild"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		return ""
	}
	return payload.Guild.Name
}

// GenerateCodes generates vanity codes based on config
func GenerateCodes(cfg types.VanityConfig) []string {
	if cfg.Custom {
		return loadCustomCodes()
	}
	charset := buildCharset(cfg)
	if len(charset) == 0 {
		charset = "abcdefghijklmnopqrstuvwxyz"
	}
	minL := cfg.MinLength
	maxL := cfg.MaxLength
	if minL < 2 {
		minL = 2
	}
	if maxL < minL {
		maxL = minL
	}

	if cfg.Exhaustive {
		return generateExhaustive(charset, minL, maxL, cfg.Prefix)
	}
	amt := cfg.Amount
	if amt <= 0 {
		amt = 1000
	}
	return generateRandom(charset, minL, maxL, cfg.Prefix, amt)
}

func buildCharset(cfg types.VanityConfig) string {
	switch cfg.Charset {
	case "alpha":
		return "abcdefghijklmnopqrstuvwxyz"
	case "alphanum":
		return "abcdefghijklmnopqrstuvwxyz0123456789"
	case "discord":
		return "abcdefghijklmnopqrstuvwxyz0123456789-"
	case "custom":
		if cfg.CustomChars != "" {
			return strings.ToLower(cfg.CustomChars)
		}
		return "abcdefghijklmnopqrstuvwxyz"
	default:
		return "abcdefghijklmnopqrstuvwxyz"
	}
}

func generateRandom(charset string, minLen, maxLen int, prefix string, amount int) []string {
	codes := make([]string, 0, amount)
	seen := make(map[string]bool)
	max := amount * 20
	for i := 0; i < max && len(codes) < amount; i++ {
		l := minLen + rand.Intn(maxLen-minLen+1)
		fill := l - len(prefix)
		if fill < 0 {
			fill = 0
		}
		var sb strings.Builder
		sb.WriteString(prefix)
		for j := 0; j < fill; j++ {
			sb.WriteByte(charset[rand.Intn(len(charset))])
		}
		code := sb.String()
		if len(code) >= 2 && !seen[code] {
			seen[code] = true
			codes = append(codes, code)
		}
	}
	return codes
}

func generateExhaustive(charset string, minLen, maxLen int, prefix string) []string {
	const cap = 500000
	var codes []string
	for l := minLen; l <= maxLen && len(codes) < cap; l++ {
		fill := l - len(prefix)
		if fill < 0 {
			continue
		}
		addCombinations(&codes, charset, prefix, fill, cap)
	}
	return codes
}

func addCombinations(out *[]string, charset, cur string, rem int, cap int) {
	if len(*out) >= cap {
		return
	}
	if rem == 0 {
		if len(cur) >= 2 {
			*out = append(*out, cur)
		}
		return
	}
	for i := 0; i < len(charset); i++ {
		addCombinations(out, charset, cur+string(charset[i]), rem-1, cap)
		if len(*out) >= cap {
			return
		}
	}
}

func loadCustomCodes() []string {
	data, err := os.ReadFile("data/vanity.txt")
	if err != nil {
		return nil
	}
	var codes []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(strings.ToLower(line))
		if line != "" && !strings.HasPrefix(line, "#") {
			codes = append(codes, line)
		}
	}
	return codes
}

// RunChecker runs the vanity checker
func RunChecker(ctx context.Context, codes []string, sessionID int64, cfg types.VanityConfig) {
	if !IsRunning() {
		SetRunning(true)
	}
	VStartTime = time.Now()
	VSessionID = sessionID
	atomic.StoreInt64(&Available, 0)
	atomic.StoreInt64(&Taken, 0)
	atomic.StoreInt64(&Errors, 0)
	atomic.StoreInt64(&VTotal, int64(len(codes)))
	atomic.StoreInt64(&vanityErrorLogCount, 0)

	globals.BroadcastLog("info", fmt.Sprintf("[Vanity] Starting: %d codes | %d threads", len(codes), cfg.Threads))

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				av := atomic.LoadInt64(&Available)
				tk := atomic.LoadInt64(&Taken)
				er := atomic.LoadInt64(&Errors)
				total := atomic.LoadInt64(&VTotal)
				globals.BroadcastEvent("vanity_stats", map[string]interface{}{
					"available": av, "taken": tk, "errors": er, "total": total,
					"remaining": total - av - tk - er, "rate": GetRate(),
					"elapsed_ms": time.Since(VStartTime).Milliseconds(), "status": Status(),
				})
				if sessionID > 0 {
					database.UpdateVanitySession(sessionID, int(av), int(tk), "running")
				}
			}
		}
	}()

	codeCh := make(chan string, cfg.Threads*2)
	var wg sync.WaitGroup
	threads := cfg.Threads
	if threads < 1 {
		threads = 1
	}

	for i := 1; i <= threads; i++ {
		wg.Add(1)
		go func(wid int) {
			defer wg.Done()
			for code := range codeCh {
				select {
				case <-ctx.Done():
					return
				default:
				}
				var taken bool
				var guild string
				var latency int
				var err error
				for rateLimitRetries := 0; rateLimitRetries <= 5; rateLimitRetries++ {
					taken, guild, latency, err = CheckCode(ctx, code)
					if !globals.IsRateLimitError(err) {
						break
					}
					if waitErr := globals.WaitForDiscordSlotFor(ctx, globals.RateRouteVanity, cfg.MinDelayMs); waitErr != nil {
						return
					}
				}
				if errors.Is(err, context.Canceled) {
					return
				}

				status := "taken"
				if err != nil {
					status = "error"
				} else if !taken {
					status = "available"
				}

				switch status {
				case "available":
					atomic.AddInt64(&Available, 1)
					globals.BroadcastLog("success", fmt.Sprintf("[Vanity] AVAILABLE: discord.gg/%s (%dms) [T%d]", code, latency, wid))
				case "taken":
					atomic.AddInt64(&Taken, 1)
					globals.BroadcastLog("info", fmt.Sprintf("[Vanity] Taken: discord.gg/%s  %s [T%d]", code, guild, wid))
				case "error":
					atomic.AddInt64(&Errors, 1)
					logVanityCheckError(code, err, wid)
				}

				if sessionID > 0 {
					database.SaveVanityResult(&types.VanityResult{
						SessionID: sessionID, Code: code, Status: status,
						GuildName: guild, CheckedAt: time.Now(), Tags: []string{}, LatencyMs: latency,
					})
				}
				globals.BroadcastEvent("vanity_result", map[string]interface{}{
					"code": code, "status": status, "guild_name": guild, "latency": latency,
				})
			}
		}(i)
	}

	go func() {
		for _, code := range codes {
			select {
			case <-ctx.Done():
				close(codeCh)
				return
			case codeCh <- code:
			}
		}
		close(codeCh)
	}()

	wg.Wait()

	av := atomic.LoadInt64(&Available)
	tk := atomic.LoadInt64(&Taken)
	er := atomic.LoadInt64(&Errors)
	total := atomic.LoadInt64(&VTotal)
	status := "completed"
	select {
	case <-ctx.Done():
		status = "stopped"
	default:
	}

	if sessionID > 0 {
		database.UpdateVanitySession(sessionID, int(av), int(tk), status)
	}
	SetStopped()
	globals.BroadcastEvent("vanity_stats", map[string]interface{}{
		"available": av, "taken": tk, "errors": er, "total": total,
		"remaining": total - av - tk - er, "rate": float64(0),
		"elapsed_ms": time.Since(VStartTime).Milliseconds(), "status": status,
	})
	globals.BroadcastEvent("vanity_stopped", map[string]interface{}{
		"status": status, "available": av, "taken": tk, "errors": er,
	})
	globals.BroadcastLog("info", fmt.Sprintf("[Vanity] %s  Available: %d | Taken: %d | Errors: %d", status, av, tk, er))
}
