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
	DiscordUsernameCheckAPI = "https://discord.com/api/v9/unique-username/username-attempt-unauthed"
	DiscordExperimentsAPI   = "https://discord.com/api/v9/experiments"
	ConfigFile              = "data/config.json"
	VanityConfigFile        = "data/vanity_config.json"
	ProxiesFile             = "data/proxies.txt"
	UsernamesFile           = "data/usernames.txt"
	BlacklistFile           = "data/blacklist.txt"
	ValidsFile              = "data/valids.txt"
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

type discordRouteLimiter struct {
	mu        sync.Mutex
	rateDelay time.Duration
	until     time.Time
	lastLog   time.Time
	lastCall  time.Time
}

var (
	Config       types.Config
	VanityConfig types.VanityConfig
	UserAgent    = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36"

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

	FingerprintMu      sync.Mutex
	DiscordFingerprint string
	usernameLimiter    discordRouteLimiter
	vanityLimiter      discordRouteLimiter

	EventCh = make(chan types.Event, 512)
)

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

func limiterFor(route string) *discordRouteLimiter {
	if route == RateRouteVanity {
		return &vanityLimiter
	}
	return &usernameLimiter
}

func resetRatePacing(route string) {
	l := limiterFor(route)
	l.mu.Lock()
	l.rateDelay = 0
	l.lastCall = time.Time{}
	l.mu.Unlock()
}

func ResetRateLimiter() {
	resetRatePacing(RateRouteUsername)
	resetRatePacing(RateRouteVanity)
}

func CooldownRemaining(route string) time.Duration {
	l := limiterFor(route)
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.until.IsZero() {
		return 0
	}
	remaining := time.Until(l.until)
	if remaining <= 0 {
		l.until = time.Time{}
		return 0
	}
	return remaining
}

func RegisterDiscordRateLimitFor(route, retryAfter string) time.Duration {
	cooldown := parseRetryAfter(retryAfter)
	if cooldown < 3*time.Second {
		cooldown = 3 * time.Second
	}
	if cooldown > time.Hour {
		cooldown = time.Hour
	}
	cooldown += 500 * time.Millisecond

	l := limiterFor(route)
	l.mu.Lock()
	until := time.Now().Add(cooldown)
	if until.After(l.until) {
		l.until = until
	}
	if l.rateDelay == 0 {
		l.rateDelay = 700 * time.Millisecond
	} else if l.rateDelay < 5*time.Second {
		l.rateDelay *= 2
		if l.rateDelay > 5*time.Second {
			l.rateDelay = 5 * time.Second
		}
	}
	shouldLog := time.Since(l.lastLog) > 5*time.Second
	if shouldLog {
		l.lastLog = time.Now()
	}
	l.mu.Unlock()

	if shouldLog {
		BroadcastLog("warn", fmt.Sprintf("Discord %s rate limit: pausing requests for %s", route, FormatDuration(cooldown)))
	}
	return cooldown
}

func RegisterDiscordRateLimit(retryAfter string) time.Duration {
	return RegisterDiscordRateLimitFor(RateRouteUsername, retryAfter)
}

func DecreaseDelayFor(route string) {
	l := limiterFor(route)
	l.mu.Lock()
	if l.rateDelay > 0 {
		l.rateDelay /= 2
	}
	l.mu.Unlock()
}

func DecreaseDelay() {
	DecreaseDelayFor(RateRouteUsername)
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

func ObserveDiscordRateLimitHeadersFor(route string, header http.Header) {
	remaining := strings.TrimSpace(header.Get("X-RateLimit-Remaining"))
	if remaining != "0" {
		return
	}
	retryAfter := header.Get("X-RateLimit-Reset-After")
	if retryAfter == "" {
		retryAfter = header.Get("Retry-After")
	}
	if retryAfter == "" {
		return
	}
	RegisterDiscordRateLimitFor(route, retryAfter)
}

func ObserveDiscordRateLimitHeaders(header http.Header) {
	ObserveDiscordRateLimitHeadersFor(RateRouteUsername, header)
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

func GetDiscordFingerprint(ctx context.Context) string {
	FingerprintMu.Lock()
	defer FingerprintMu.Unlock()
	if DiscordFingerprint != "" {
		return DiscordFingerprint
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, DiscordExperimentsAPI, nil)
	if err != nil {
		return ""
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")
	req.Header.Set("Referer", "https://discord.com/register")
	req.Header.Set("User-Agent", UserAgent)

	client := &http.Client{Timeout: 15 * time.Second}
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
	DiscordFingerprint = payload.Fingerprint
	return DiscordFingerprint
}

func WaitForDiscordSlotFor(ctx context.Context, route string, configuredMinDelayMs int) error {
	const baseDelay = 350 * time.Millisecond
	l := limiterFor(route)

	for {
		l.mu.Lock()
		if l.until.IsZero() {
			l.mu.Unlock()
			break
		}
		wait := time.Until(l.until)
		if wait <= 0 {
			l.until = time.Time{}
			l.mu.Unlock()
			break
		}
		l.mu.Unlock()
		if err := waitContext(ctx, wait); err != nil {
			return err
		}
	}

	spacing := baseDelay
	if configuredMinDelayMs > 0 && time.Duration(configuredMinDelayMs)*time.Millisecond > spacing {
		spacing = time.Duration(configuredMinDelayMs) * time.Millisecond
	}

	for {
		l.mu.Lock()
		if l.rateDelay > spacing {
			spacing = l.rateDelay
		}
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

func WaitForDiscordSlot(ctx context.Context, configuredMinDelayMs int) error {
	return WaitForDiscordSlotFor(ctx, RateRouteUsername, configuredMinDelayMs)
}

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
