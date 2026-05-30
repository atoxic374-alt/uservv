package globals

import (
        "context"
        "encoding/json"
        "fmt"
        "math/rand"
        "net/http"
        "os"
        "strconv"
        "strings"
        "sync"
        "sync/atomic"
        "time"
        "users/logger"
        "users/types"
)

const (
        DiscordUsernameCheckAPI       = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"
        DiscordUsernameCheckAPIAuthed = "https://discord.com/api/v9/unique-username/username-attempt"
        DiscordExperimentsAPI         = "https://discord.com/api/v9/experiments"
        DiscordUsersMeAPI             = "https://discord.com/api/v9/users/@me"
        ConfigFile                    = "data/config.json"
        VanityConfigFile              = "data/vanity_config.json"
        ProxiesFile                   = "data/proxies.txt"
        UsernamesFile                 = "data/usernames.txt"
        BlacklistFile                 = "data/blacklist.txt"
        ValidsFile                    = "data/valids.txt"
)

const (
        CheckerStateIdle int32 = iota
        CheckerStateRunning
        CheckerStateStopping
)

const (
        RateRouteUsername = "username"
        RateRouteVanity   = "vanity"
)

// Pool of realistic Chrome User-Agents — rotated per request
var userAgentPool = []string{
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:137.0) Gecko/20100101 Firefox/137.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
}

// UserAgent — default UA (kept for backwards compat; prefer RandomUserAgent())
var UserAgent = userAgentPool[0]

// RandomUserAgent picks a random UA from the pool
func RandomUserAgent() string {
        return userAgentPool[rand.Intn(len(userAgentPool))]
}

// ─── Fingerprint pool ───────────────────────────────────────────────────────

const fingerprintPoolSize = 8

type fingerprintEntry struct {
        value   string
        fetched time.Time
}

var (
        fpPool  [fingerprintPoolSize]fingerprintEntry
        fpMu    sync.Mutex
        fpRound int64 // atomic round-robin counter
)

// GetDiscordFingerprint returns a fingerprint from the rotating pool.
// Each slot is refreshed after 10 minutes.
func GetDiscordFingerprint(ctx context.Context) string {
        idx := int(atomic.AddInt64(&fpRound, 1) % fingerprintPoolSize)
        fpMu.Lock()
        e := &fpPool[idx]
        if e.value != "" && time.Since(e.fetched) < 10*time.Minute {
                v := e.value
                fpMu.Unlock()
                return v
        }
        fpMu.Unlock()

        fp := fetchFingerprint(ctx)
        if fp != "" {
                fpMu.Lock()
                fpPool[idx] = fingerprintEntry{value: fp, fetched: time.Now()}
                fpMu.Unlock()
        }
        return fp
}

func fetchFingerprint(ctx context.Context) string {
        ua := RandomUserAgent()
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, DiscordExperimentsAPI, nil)
        if err != nil {
                return ""
        }
        req.Header.Set("Accept", "application/json")
        req.Header.Set("Accept-Language", "en-US,en;q=0.9")
        req.Header.Set("Referer", "https://discord.com/register")
        req.Header.Set("User-Agent", ua)

        client := &http.Client{Timeout: 10 * time.Second}
        resp, err := client.Do(req)
        if err != nil {
                return ""
        }
        defer resp.Body.Close()
        var payload struct {
                Fingerprint string `json:"fingerprint"`
        }
        if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
                return ""
        }
        return payload.Fingerprint
}

// ─── Per-proxy / per-route rate limiters ───────────────────────────────────
// Key format:  "<route>:<proxyKey>"
// proxyKey is the proxy URL string, or "direct" when no proxy is used.
// Each proxy IP has its own independent rate-limit bucket, matching how
// Discord tracks limits on the server side.

type discordRouteLimiter struct {
        mu        sync.Mutex
        rateDelay time.Duration
        until     time.Time
        lastLog   time.Time
        lastCall  time.Time
}

var (
        limitersMu sync.Mutex
        limitersMap = map[string]*discordRouteLimiter{}
)

// ProxyKeyDirect is the key used when no proxy is configured.
const ProxyKeyDirect = "direct"

func limiterFor(route, proxyKey string) *discordRouteLimiter {
        if proxyKey == "" {
                proxyKey = ProxyKeyDirect
        }
        key := route + ":" + proxyKey
        limitersMu.Lock()
        defer limitersMu.Unlock()
        if l, ok := limitersMap[key]; ok {
                return l
        }
        l := &discordRouteLimiter{}
        limitersMap[key] = l
        return l
}

// ─── Shared state ──────────────────────────────────────────────────────────

var (
        Config       types.Config
        VanityConfig types.VanityConfig

        Proxies   = []string{}
        Usernames = []string{}
        BlackList = []string{}

        ValidUsernames   int64
        InvalidUsernames int64

        blacklistMutex     sync.Mutex
        validUsernameMutex sync.Mutex

        CurrentSessionID int64
        CheckerRunning   int32
        CheckerCancel    context.CancelFunc
        CheckerStartTime time.Time

        // Kept for backwards compat with code that still references it directly
        FingerprintMu      sync.Mutex
        DiscordFingerprint string

        EventCh = make(chan types.Event, 512)
)

// ─── Event broadcasting ────────────────────────────────────────────────────

func BroadcastEvent(eventType string, data interface{}) {
        select {
        case EventCh <- types.Event{Type: eventType, Data: data}:
        default:
        }
}

func BroadcastLog(level, msg string) {
        BroadcastEvent("log", types.LogData{
                Level:   level,
                Message: msg,
                Time:    time.Now().Format("15:04:05"),
        })
}

// ─── Checker state ─────────────────────────────────────────────────────────

func IsCheckerRunning() bool { return atomic.LoadInt32(&CheckerRunning) != CheckerStateIdle }

func CheckerStatus() string {
        switch atomic.LoadInt32(&CheckerRunning) {
        case CheckerStateRunning:
                return "running"
        case CheckerStateStopping:
                return "stopping"
        default:
                return "idle"
        }
}

func TryStartChecker() bool {
        return atomic.CompareAndSwapInt32(&CheckerRunning, CheckerStateIdle, CheckerStateRunning)
}

func RequestCheckerStop() bool {
        for {
                state := atomic.LoadInt32(&CheckerRunning)
                if state == CheckerStateIdle {
                        return false
                }
                if state == CheckerStateStopping {
                        return true
                }
                if atomic.CompareAndSwapInt32(&CheckerRunning, state, CheckerStateStopping) {
                        return true
                }
        }
}

func SetCheckerStopped() { atomic.StoreInt32(&CheckerRunning, CheckerStateIdle) }

func SetCheckerRunning(v bool) {
        if v {
                atomic.StoreInt32(&CheckerRunning, CheckerStateRunning)
        } else {
                SetCheckerStopped()
        }
}

func GetCurrentRate() float64 {
        if !IsCheckerRunning() {
                return 0
        }
        elapsed := time.Since(CheckerStartTime).Seconds()
        if elapsed <= 0 {
                return 0
        }
        return float64(atomic.LoadInt64(&ValidUsernames)+atomic.LoadInt64(&InvalidUsernames)) / elapsed
}

// ─── Rate limit helpers ────────────────────────────────────────────────────

func waitContext(ctx context.Context, d time.Duration) error {
        if d <= 0 {
                return nil
        }
        timer := time.NewTimer(d)
        defer timer.Stop()
        select {
        case <-ctx.Done():
                return ctx.Err()
        case <-timer.C:
                return nil
        }
}

func parseRetryAfter(value string) time.Duration {
        value = strings.TrimSpace(value)
        if value == "" {
                return 0
        }
        if sec, err := strconv.ParseFloat(value, 64); err == nil {
                return time.Duration(sec * float64(time.Second))
        }
        if t, err := time.Parse(time.RFC1123, value); err == nil {
                return time.Until(t)
        }
        return 0
}

// ResetRateLimiter clears all per-proxy limiters.
func ResetRateLimiter() {
        limitersMu.Lock()
        limitersMap = map[string]*discordRouteLimiter{}
        limitersMu.Unlock()
}

func CooldownRemaining(route string) time.Duration {
        // Return the longest cooldown across all limiters for this route
        limitersMu.Lock()
        var max time.Duration
        for key, l := range limitersMap {
                if !strings.HasPrefix(key, route+":") {
                        continue
                }
                l.mu.Lock()
                rem := time.Until(l.until)
                l.mu.Unlock()
                if rem > max {
                        max = rem
                }
        }
        limitersMu.Unlock()
        return max
}

// RegisterDiscordRateLimitFor records a 429 hit for a specific route+proxy.
func RegisterDiscordRateLimitFor(route, proxyKey, retryAfter string) time.Duration {
        cooldown := parseRetryAfter(retryAfter)
        if cooldown < 2*time.Second {
                cooldown = 2 * time.Second
        }
        if cooldown > 30*time.Minute {
                cooldown = 30 * time.Minute
        }
        cooldown += 300 * time.Millisecond // small safety buffer

        l := limiterFor(route, proxyKey)
        l.mu.Lock()
        until := time.Now().Add(cooldown)
        if until.After(l.until) {
                l.until = until
        }
        // Increase per-proxy delay on rate-limit hit
        if l.rateDelay == 0 {
                l.rateDelay = 500 * time.Millisecond
        } else if l.rateDelay < 5*time.Second {
                l.rateDelay = l.rateDelay * 3 / 2
                if l.rateDelay > 5*time.Second {
                        l.rateDelay = 5 * time.Second
                }
        }
        shouldLog := time.Since(l.lastLog) > 3*time.Second
        if shouldLog {
                l.lastLog = time.Now()
        }
        l.mu.Unlock()

        if shouldLog {
                pk := proxyKey
                if len(pk) > 30 {
                        pk = pk[:30] + "…"
                }
                if pk == ProxyKeyDirect {
                        pk = "direct"
                }
                BroadcastLog("warn", fmt.Sprintf("Discord %s rate limit [%s]: pausing %s", route, pk, FormatDuration(cooldown)))
        }
        return cooldown
}

// RegisterDiscordRateLimit — backwards-compat wrapper (direct connection, username route)
func RegisterDiscordRateLimit(retryAfter string) time.Duration {
        return RegisterDiscordRateLimitFor(RateRouteUsername, ProxyKeyDirect, retryAfter)
}

// DecreaseDelayFor reduces the per-proxy delay after a successful request.
func DecreaseDelayFor(route, proxyKey string) {
        l := limiterFor(route, proxyKey)
        l.mu.Lock()
        if l.rateDelay > 0 {
                l.rateDelay = l.rateDelay * 2 / 3
                if l.rateDelay < 50*time.Millisecond {
                        l.rateDelay = 0
                }
        }
        l.mu.Unlock()
}

// DecreaseDelay — backwards-compat
func DecreaseDelay() {
        DecreaseDelayFor(RateRouteUsername, ProxyKeyDirect)
}

func FormatDuration(d time.Duration) string {
        if d < time.Minute {
                return d.Round(time.Second).String()
        }
        seconds := int(d.Round(time.Second).Seconds())
        minutes := seconds / 60
        seconds = seconds % 60
        if seconds == 0 {
                return fmt.Sprintf("%dm", minutes)
        }
        return fmt.Sprintf("%dm%02ds", minutes, seconds)
}

// ObserveDiscordRateLimitHeadersFor reads rate-limit headers and registers a
// limit proactively when X-RateLimit-Remaining reaches 0 or 1.
func ObserveDiscordRateLimitHeadersFor(route, proxyKey string, header http.Header) {
        remaining := strings.TrimSpace(header.Get("X-RateLimit-Remaining"))
        rem, err := strconv.Atoi(remaining)
        if err != nil || rem > 1 {
                return
        }
        retryAfter := header.Get("X-RateLimit-Reset-After")
        if retryAfter == "" {
                retryAfter = header.Get("Retry-After")
        }
        if retryAfter == "" {
                return
        }
        RegisterDiscordRateLimitFor(route, proxyKey, retryAfter)
}

// ObserveDiscordRateLimitHeaders — backwards-compat
func ObserveDiscordRateLimitHeaders(header http.Header) {
        ObserveDiscordRateLimitHeadersFor(RateRouteUsername, ProxyKeyDirect, header)
}

func RetryAfterValue(header http.Header, body []byte) string {
        if v := header.Get("Retry-After"); v != "" {
                return v
        }
        if v := header.Get("X-RateLimit-Reset-After"); v != "" {
                return v
        }
        var payload struct {
                RetryAfter float64 `json:"retry_after"`
        }
        if len(body) > 0 && json.Unmarshal(body, &payload) == nil && payload.RetryAfter > 0 {
                return fmt.Sprintf("%.3f", payload.RetryAfter)
        }
        return ""
}

// WaitForDiscordSlotFor waits until the given route+proxy limiter has a free
// slot, respecting any active cooldown and per-proxy pacing.
// proxyKey should be the proxy URL string, "direct", or "tok:<prefix>" for token mode.
func WaitForDiscordSlotFor(ctx context.Context, route, proxyKey string, configuredMinDelayMs int) error {
        // Token mode: Discord's per-token bucket allows ~10 req/s → 100ms floor.
        // IP (direct/proxy) mode: 5 req/s → 200ms floor.
        // Adaptive rate limiter will back off further if we see 429s.
        safeMin := 200 * time.Millisecond
        if strings.HasPrefix(proxyKey, "tok:") {
                safeMin = 100 * time.Millisecond
        }

        l := limiterFor(route, proxyKey)

        // 1. Wait out any hard cooldown (429 response)
        for {
                l.mu.Lock()
                wait := time.Until(l.until)
                l.mu.Unlock()
                if wait <= 0 {
                        break
                }
                if err := waitContext(ctx, wait); err != nil {
                        return err
                }
        }

        // 2. Pace requests — respect both configured min and adaptive rateDelay,
        //    with a small random jitter (±15%) to avoid synchronized bursts.
        for {
                l.mu.Lock()
                spacing := safeMin
                if configuredMinDelayMs > 0 {
                        if d := time.Duration(configuredMinDelayMs) * time.Millisecond; d > spacing {
                                spacing = d
                        }
                }
                if l.rateDelay > spacing {
                        spacing = l.rateDelay
                }
                // Add ±15% jitter
                jitter := time.Duration(rand.Int63n(int64(spacing/7+1))) - spacing/14
                spacing += jitter

                wait := time.Duration(0)
                if !l.lastCall.IsZero() {
                        wait = spacing - time.Since(l.lastCall)
                }
                if wait <= 0 {
                        l.lastCall = time.Now()
                        l.mu.Unlock()
                        return nil
                }
                l.mu.Unlock()

                if err := waitContext(ctx, wait); err != nil {
                        return err
                }
        }
}

// WaitForDiscordSlot — backwards-compat (direct, username route)
func WaitForDiscordSlot(ctx context.Context, configuredMinDelayMs int) error {
        return WaitForDiscordSlotFor(ctx, RateRouteUsername, ProxyKeyDirect, configuredMinDelayMs)
}

// ─── Error helpers ─────────────────────────────────────────────────────────

func CompactCheckError(err error) string {
        if err == nil {
                return ""
        }
        msg := err.Error()
        lower := strings.ToLower(msg)
        switch {
        case strings.Contains(lower, "tor is not an http proxy"):
                return "proxy type mismatch: SOCKS/Tor proxy was used as HTTP"
        case strings.Contains(lower, "proxyconnect"):
                return "proxy connection failed"
        case strings.Contains(lower, "no route to host"):
                return "proxy host unreachable"
        case strings.Contains(lower, "connection refused"):
                return "proxy refused the connection"
        case strings.Contains(lower, "connection reset"):
                return "proxy reset the connection"
        case strings.Contains(lower, "unexpected eof"):
                return "proxy closed the connection early"
        case strings.Contains(lower, "deadline exceeded") || strings.Contains(lower, "client.timeout"):
                return "request timed out"
        case strings.Contains(lower, "rate limited") || strings.Contains(lower, "429"):
                return "rate limited by Discord"
        }
        return msg
}

func IsRateLimitError(err error) bool {
        return err != nil && strings.Contains(strings.ToLower(err.Error()), "rate limited")
}

func ShouldBroadcastCheckError(counter *int64) (int64, bool) {
        n := atomic.AddInt64(counter, 1)
        return n, n <= 3 || n%25 == 0
}

// ─── Username generators ───────────────────────────────────────────────────

func GenerateRandomUsername(length int) (string, error) {
        return GenerateRandomUsernameFromCharset(length, "abcdefghijklmnopqrstuvwxyz0123456789")
}

func GenerateRandomUsernameFromCharset(length int, charset string) (string, error) {
        if length <= 0 {
                return "", fmt.Errorf("length must be > 0")
        }
        if charset == "" {
                return "", fmt.Errorf("charset must not be empty")
        }
        b := make([]byte, length)
        for i := range b {
                b[i] = charset[rand.Intn(len(charset))]
        }
        return string(b), nil
}

func usernameCharset() string {
        switch Config.Usernames.Charset {
        case "alpha":
                return "abcdefghijklmnopqrstuvwxyz"
        case "num":
                return "0123456789"
        case "discord":
                return "abcdefghijklmnopqrstuvwxyz0123456789_."
        case "custom":
                if Config.Usernames.CustomChars != "" {
                        return strings.ToLower(Config.Usernames.CustomChars)
                }
                return "abcdefghijklmnopqrstuvwxyz0123456789"
        default:
                return "abcdefghijklmnopqrstuvwxyz0123456789"
        }
}

func randomUsernameLength() int {
        mode := Config.Usernames.LengthMode
        length := Config.Usernames.Length
        if length <= 0 {
                length = 3
        }
        switch mode {
        case "upto":
                maxLen := Config.Usernames.MaxLength
                if maxLen <= 0 {
                        maxLen = length
                }
                if maxLen < 2 {
                        maxLen = 2
                }
                return 2 + rand.Intn(maxLen-2+1)
        case "range":
                minLen := Config.Usernames.MinLength
                maxLen := Config.Usernames.MaxLength
                if minLen <= 0 {
                        minLen = 2
                }
                if maxLen < minLen {
                        maxLen = minLen
                }
                return minLen + rand.Intn(maxLen-minLen+1)
        default:
                return length
        }
}

// ─── Config I/O ────────────────────────────────────────────────────────────

func LoadConfig() error {
        f, err := os.Open(ConfigFile)
        if err != nil {
                return err
        }
        defer f.Close()
        return json.NewDecoder(f).Decode(&Config)
}

func SaveConfig() error {
        data, err := json.MarshalIndent(Config, "", "    ")
        if err != nil {
                return err
        }
        return os.WriteFile(ConfigFile, data, 0644)
}

func LoadVanityConfig() error {
        f, err := os.Open(VanityConfigFile)
        if err != nil {
                return err
        }
        defer f.Close()
        return json.NewDecoder(f).Decode(&VanityConfig)
}

func SaveVanityConfig() error {
        data, err := json.MarshalIndent(VanityConfig, "", "    ")
        if err != nil {
                return err
        }
        return os.WriteFile(VanityConfigFile, data, 0644)
}

func LoadProxies() error {
        file, err := os.ReadFile(ProxiesFile)
        if err != nil {
                if os.IsNotExist(err) {
                        return nil
                }
                return err
        }
        Proxies = []string{}
        for _, line := range strings.Split(string(file), "\n") {
                t := strings.TrimSpace(line)
                if t == "" || strings.HasPrefix(t, "#") || !strings.Contains(t, ":") {
                        continue
                }
                if !strings.HasPrefix(strings.ToLower(t), "http") && !strings.HasPrefix(strings.ToLower(t), "socks") {
                        t = "http://" + t
                }
                Proxies = append(Proxies, t)
        }
        return nil
}

func LoadUsernames() error {
        Usernames = []string{}
        if !Config.Usernames.Custom {
                charset := usernameCharset()
                for i := 0; i < Config.Usernames.Amount; i++ {
                        var u string
                        for attempt := 0; attempt < 50; attempt++ {
                                candidate, err := GenerateRandomUsernameFromCharset(randomUsernameLength(), charset)
                                if err != nil {
                                        return err
                                }
                                candidate = strings.ToLower(candidate)
                                if !strings.Contains(candidate, "..") {
                                        u = candidate
                                        break
                                }
                        }
                        if u == "" {
                                i--
                                continue
                        }
                        Usernames = append(Usernames, u)
                }
        } else {
                file, err := os.ReadFile(UsernamesFile)
                if err != nil {
                        return err
                }
                for _, line := range strings.Split(string(file), "\n") {
                        line = strings.TrimSpace(line)
                        if line != "" && !strings.HasPrefix(line, "#") {
                                Usernames = append(Usernames, line)
                        }
                }
        }
        return nil
}

func LoadBlackList() error {
        file, err := os.ReadFile(BlacklistFile)
        if err != nil {
                return err
        }
        for _, line := range strings.Split(string(file), "\n") {
                line = strings.TrimSpace(line)
                if line != "" {
                        BlackList = append(BlackList, line)
                }
        }
        return nil
}

func SaveBlackList(username string) error {
        blacklistMutex.Lock()
        defer blacklistMutex.Unlock()
        for _, b := range BlackList {
                if b == username {
                        return nil
                }
        }
        BlackList = append(BlackList, username)
        f, err := os.Create(BlacklistFile)
        if err != nil {
                return err
        }
        defer f.Close()
        for _, b := range BlackList {
                f.WriteString(b + "\n")
        }
        return nil
}

func SaveValidUser(username string) error {
        validUsernameMutex.Lock()
        defer validUsernameMutex.Unlock()
        f, err := os.OpenFile(ValidsFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
                return err
        }
        defer f.Close()
        _, err = f.WriteString(username + "\n")
        return err
}

func SendDiscordWebhook(username string) {
        if Config.Webhook == "" {
                return
        }
        logger.Info(fmt.Sprintf("Webhook: sending for [%s]", username))
}
