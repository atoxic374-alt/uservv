package types

import "time"

type Config struct {
        Usernames struct {
                Custom      bool   `json:"custom"`
                Amount      int    `json:"amount"`
                Length      int    `json:"length"`
                LengthMode  string `json:"length_mode,omitempty"`
                MinLength   int    `json:"min_length,omitempty"`
                MaxLength   int    `json:"max_length,omitempty"`
                Charset     string `json:"charset,omitempty"`
                CustomChars string `json:"custom_chars,omitempty"`
        } `json:"usernames"`
        Retry struct {
                Enabled     bool `json:"enabled"`
                MaxAttempts int  `json:"max_attempts"`
        } `json:"retry"`
        Threads        int    `json:"threads"`
        Timeout        int    `json:"timeout"`
        Webhook        string `json:"webhook"`
        DryRun         bool   `json:"dry_run"`
        DoubleVerify   bool   `json:"double_verify"`
        AdaptiveRate   bool   `json:"adaptive_rate"`
        SkipDuplicates bool   `json:"skip_duplicates"`
        MinDelayMs     int    `json:"min_delay_ms"`
        DiscordToken   string `json:"discord_token,omitempty"`
        AutoRotateIP   bool   `json:"auto_rotate_ip"`
}

type VanityConfig struct {
        Custom       bool   `json:"custom"`
        Exhaustive   bool   `json:"exhaustive"`
        Amount       int    `json:"amount"`
        MinLength    int    `json:"min_length"`
        MaxLength    int    `json:"max_length"`
        Prefix       string `json:"prefix"`
        Charset      string `json:"charset"`
        CustomChars  string `json:"custom_chars"`
        Threads      int    `json:"threads"`
        Timeout      int    `json:"timeout"`
        AdaptiveRate bool   `json:"adaptive_rate"`
        SkipDups     bool   `json:"skip_duplicates"`
        MinDelayMs   int    `json:"min_delay_ms"`
}

type UsernameRequest struct {
        Username string `json:"username"`
}

type UsernameResponse struct {
        Taken bool `json:"taken"`
}

type Proxy struct {
        ID           int64     `json:"id"`
        URL          string    `json:"url"`
        Type         string    `json:"type"`
        Healthy      bool      `json:"healthy"`
        LastChecked  time.Time `json:"last_checked"`
        SuccessCount int       `json:"success_count"`
        FailCount    int       `json:"fail_count"`
        AvgLatencyMs int       `json:"avg_latency_ms"`
}

type Session struct {
        ID         int64      `json:"id"`
        StartedAt  time.Time  `json:"started_at"`
        FinishedAt *time.Time `json:"finished_at"`
        Total      int        `json:"total"`
        Valid      int        `json:"valid"`
        Invalid    int        `json:"invalid"`
        Mode       string     `json:"mode"`
        Status     string     `json:"status"`
}

type VanitySession struct {
        ID         int64      `json:"id"`
        StartedAt  time.Time  `json:"started_at"`
        FinishedAt *time.Time `json:"finished_at"`
        Total      int        `json:"total"`
        Available  int        `json:"available"`
        Taken      int        `json:"taken"`
        Status     string     `json:"status"`
        Config     string     `json:"config"`
}

type Result struct {
        ID        int64     `json:"id"`
        SessionID int64     `json:"session_id"`
        Username  string    `json:"username"`
        Status    string    `json:"status"`
        CheckedAt time.Time `json:"checked_at"`
        Tags      []string  `json:"tags"`
        LatencyMs int       `json:"latency_ms"`
}

type VanityResult struct {
        ID        int64     `json:"id"`
        SessionID int64     `json:"session_id"`
        Code      string    `json:"code"`
        Status    string    `json:"status"`
        GuildName string    `json:"guild_name"`
        CheckedAt time.Time `json:"checked_at"`
        Tags      []string  `json:"tags"`
        LatencyMs int       `json:"latency_ms"`
}

type Event struct {
        Type string      `json:"type"`
        Data interface{} `json:"data"`
}

type StatsData struct {
        Valid     int64   `json:"valid"`
        Invalid   int64   `json:"invalid"`
        Total     int64   `json:"total"`
        Remaining int64   `json:"remaining"`
        Rate      float64 `json:"rate"`
        ElapsedMs int64   `json:"elapsed_ms"`
        Status    string  `json:"status"`
}

type LogData struct {
        Level   string `json:"level"`
        Message string `json:"message"`
        Time    string `json:"time"`
}
