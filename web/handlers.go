package web

import (
        "context"
        "encoding/json"
        "fmt"
        "net/http"
        "strconv"
        "strings"
        "sync"
        "sync/atomic"
        "time"
        "users/checker"
        "users/database"
        "users/globals"
        "users/proxy"
        "users/types"
        "users/vanity"
)

var startMu sync.Mutex

func jsonOK(w http.ResponseWriter, data interface{}) {
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(data)
}

func jsonErr(w http.ResponseWriter, msg string, code int) {
        w.Header().Set("Content-Type", "application/json")
        w.WriteHeader(code)
        json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

//  Status

func HandleStatus(w http.ResponseWriter, r *http.Request) {
        valid := atomic.LoadInt64(&globals.ValidUsernames)
        invalid := atomic.LoadInt64(&globals.InvalidUsernames)
        total := int64(len(globals.Usernames))
        jsonOK(w, map[string]interface{}{
                "running":        globals.IsCheckerRunning(),
                "checker_status": globals.CheckerStatus(),
                "vanity_running": vanity.IsRunning(),
                "vanity_status":  vanity.Status(),
                "username_cooldown_ms": func() int64 {
                        if d := globals.CooldownRemaining(globals.RateRouteUsername); d > 0 {
                                return d.Milliseconds()
                        }
                        return 0
                }(),
                "vanity_cooldown_ms": func() int64 {
                        if d := globals.CooldownRemaining(globals.RateRouteVanity); d > 0 {
                                return d.Milliseconds()
                        }
                        return 0
                }(),
                "valid":     valid,
                "invalid":   invalid,
                "total":     total,
                "remaining": total - valid - invalid,
                "rate":      globals.GetCurrentRate(),
                "elapsed_ms": func() int64 {
                        if globals.IsCheckerRunning() {
                                return time.Since(globals.CheckerStartTime).Milliseconds()
                        }
                        return 0
                }(),
                "session_id": globals.CurrentSessionID,
                "db_stats":   database.GetDBStats(),
                "token_active": globals.Config.DiscordToken != "",
                "rate_limit_per_sec": func() int {
                        if globals.Config.DiscordToken != "" {
                                return 10
                        }
                        return 5
                }(),
                "auto_ip_count": proxy.Auto.Count(),
        })
}

//  Username Config

func HandleGetConfig(w http.ResponseWriter, r *http.Request) { jsonOK(w, globals.Config) }

func HandleSetConfig(w http.ResponseWriter, r *http.Request) {
        if globals.IsCheckerRunning() {
                jsonErr(w, "stop the checker first", http.StatusConflict)
                return
        }
        var cfg types.Config
        if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
                jsonErr(w, err.Error(), http.StatusBadRequest)
                return
        }
        globals.Config = cfg
        if err := globals.SaveConfig(); err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        globals.BroadcastLog("info", "Configuration saved")
        jsonOK(w, map[string]string{"status": "ok"})
}

//  Username Checker

func HandleStart(w http.ResponseWriter, r *http.Request) {
        startMu.Lock()
        if vanity.IsRunning() {
                startMu.Unlock()
                jsonErr(w, "stop vanity checker first to avoid Discord rate limits", http.StatusConflict)
                return
        }
        if !globals.TryStartChecker() {
                startMu.Unlock()
                jsonErr(w, "checker already "+globals.CheckerStatus(), http.StatusConflict)
                return
        }
        if remaining := globals.CooldownRemaining(globals.RateRouteUsername); remaining > 0 {
                startMu.Unlock()
                globals.SetCheckerStopped()
                jsonErr(w, "Discord username endpoint is cooling down. Wait "+globals.FormatDuration(remaining)+" before starting again.", http.StatusTooManyRequests)
                return
        }
        startMu.Unlock()
        started := false
        defer func() {
                if !started {
                        globals.SetCheckerStopped()
                }
        }()

        // Parse optional body for force flag
        var body struct {
                Force bool `json:"force"`
        }
        json.NewDecoder(r.Body).Decode(&body)

        if err := globals.LoadUsernames(); err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        if len(globals.Usernames) == 0 {
                if globals.Config.Usernames.Custom {
                        jsonErr(w, "data/usernames.txt has no usernames. Add one username per line or switch Source Mode to Generate.", http.StatusBadRequest)
                        return
                }
                jsonErr(w, "no usernames generated. Check amount, length, and charset settings.", http.StatusBadRequest)
                return
        }
        usernames := make([]string, len(globals.Usernames))
        copy(usernames, globals.Usernames)

        // Always skip already-checked usernames unless force=true
        if !body.Force {
                if checked, err := database.GetCheckedUsernames(); err == nil && len(checked) > 0 {
                        filtered := usernames[:0]
                        skipped := 0
                        for _, u := range usernames {
                                if !checked[u] {
                                        filtered = append(filtered, u)
                                } else {
                                        skipped++
                                }
                        }
                        usernames = filtered
                        if skipped > 0 {
                                globals.BroadcastLog("info", fmt.Sprintf("Skipped %d already-checked usernames (use Force to re-check)", skipped))
                        }
                }
        } else {
                globals.BroadcastLog("warn", fmt.Sprintf("Force mode: re-checking all %d usernames including previously checked", len(usernames)))
        }

        if len(usernames) == 0 {
                jsonErr(w, "all usernames already checked — use Force Re-check to check again", http.StatusBadRequest)
                return
        }

        proxy.Default.Reload()
        if proxy.Default.Count() == 0 {
                globals.BroadcastLog("info", "No proxies configured; using direct connection")
        }

        mode := "random"
        if globals.Config.Usernames.Custom {
                mode = "custom"
        }
        sessionID, _ := database.CreateSession(mode, len(usernames))
        globals.CurrentSessionID = sessionID
        ctx, cancel := context.WithCancel(context.Background())
        globals.CheckerCancel = cancel
        started = true
        go checker.RunChecker(ctx, usernames, sessionID)
        jsonOK(w, map[string]interface{}{"status": "started", "total": len(usernames), "session_id": sessionID})
}

func HandleStop(w http.ResponseWriter, r *http.Request) {
        alreadyStopping := globals.CheckerStatus() == "stopping"
        if !globals.RequestCheckerStop() {
                jsonErr(w, "not running", http.StatusConflict)
                return
        }
        if globals.CheckerCancel != nil {
                globals.CheckerCancel()
        }
        if !alreadyStopping {
                globals.BroadcastLog("warn", "Checker stop requested")
        }
        jsonOK(w, map[string]string{"status": globals.CheckerStatus()})
}

//  Vanity Config

func HandleGetVanityConfig(w http.ResponseWriter, r *http.Request) { jsonOK(w, globals.VanityConfig) }

func HandleSetVanityConfig(w http.ResponseWriter, r *http.Request) {
        if vanity.IsRunning() {
                jsonErr(w, "stop vanity checker first", http.StatusConflict)
                return
        }
        var cfg types.VanityConfig
        if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
                jsonErr(w, err.Error(), http.StatusBadRequest)
                return
        }
        globals.VanityConfig = cfg
        if err := globals.SaveVanityConfig(); err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        globals.BroadcastLog("info", "Vanity config saved")
        jsonOK(w, map[string]string{"status": "ok"})
}

//  Vanity Checker

func HandleVanityStart(w http.ResponseWriter, r *http.Request) {
        startMu.Lock()
        if globals.IsCheckerRunning() {
                startMu.Unlock()
                jsonErr(w, "stop username checker first to avoid Discord rate limits", http.StatusConflict)
                return
        }
        if !vanity.TryStart() {
                startMu.Unlock()
                jsonErr(w, "vanity checker already "+vanity.Status(), http.StatusConflict)
                return
        }
        if remaining := globals.CooldownRemaining(globals.RateRouteVanity); remaining > 0 {
                startMu.Unlock()
                vanity.SetStopped()
                jsonErr(w, "Discord vanity endpoint is cooling down. Wait "+globals.FormatDuration(remaining)+" before starting again.", http.StatusTooManyRequests)
                return
        }
        startMu.Unlock()
        started := false
        defer func() {
                if !started {
                        vanity.SetStopped()
                }
        }()
        cfg := globals.VanityConfig
        codes := vanity.GenerateCodes(cfg)
        if len(codes) == 0 {
                jsonErr(w, "no codes generated  check configuration", http.StatusBadRequest)
                return
        }

        if cfg.SkipDups {
                if checked, err := database.GetCheckedVanityCodes(); err == nil && len(checked) > 0 {
                        filtered := codes[:0]
                        skipped := 0
                        for _, c := range codes {
                                if !checked[c] {
                                        filtered = append(filtered, c)
                                } else {
                                        skipped++
                                }
                        }
                        codes = filtered
                        if skipped > 0 {
                                globals.BroadcastLog("info", fmt.Sprintf("[Vanity] Skipped %d already-checked codes", skipped))
                        }
                }
        }
        if len(codes) == 0 {
                jsonErr(w, "all codes already checked (skip duplicates)", http.StatusBadRequest)
                return
        }

        proxy.Default.Reload()
        if proxy.Default.Count() == 0 {
                globals.BroadcastLog("info", "[Vanity] No proxies configured; using direct connection")
        }

        cfgJSON, _ := json.Marshal(cfg)
        sessionID, _ := database.CreateVanitySession(len(codes), string(cfgJSON))
        ctx, cancel := context.WithCancel(context.Background())
        vanity.VCancel = cancel
        started = true
        go vanity.RunChecker(ctx, codes, sessionID, cfg)
        jsonOK(w, map[string]interface{}{"status": "started", "total": len(codes), "session_id": sessionID})
}

func HandleVanityStop(w http.ResponseWriter, r *http.Request) {
        alreadyStopping := vanity.Status() == "stopping"
        if !vanity.RequestStop() {
                jsonErr(w, "not running", http.StatusConflict)
                return
        }
        if vanity.VCancel != nil {
                vanity.VCancel()
        }
        if !alreadyStopping {
                globals.BroadcastLog("warn", "[Vanity] Stop requested")
        }
        jsonOK(w, map[string]string{"status": vanity.Status()})
}

func HandleVanityStatus(w http.ResponseWriter, r *http.Request) {
        av := atomic.LoadInt64(&vanity.Available)
        tk := atomic.LoadInt64(&vanity.Taken)
        total := atomic.LoadInt64(&vanity.VTotal)
        jsonOK(w, map[string]interface{}{
                "running":   vanity.IsRunning(),
                "available": av,
                "taken":     tk,
                "total":     total,
                "remaining": total - av - tk,
                "rate":      vanity.GetRate(),
                "elapsed_ms": func() int64 {
                        if vanity.IsRunning() {
                                return time.Since(vanity.VStartTime).Milliseconds()
                        }
                        return 0
                }(),
                "session_id": vanity.VSessionID,
        })
}

func HandleGetVanityResults(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()
        sessionID, _ := strconv.ParseInt(q.Get("session_id"), 10, 64)
        status := q.Get("status")
        search := q.Get("search")
        limit, _ := strconv.Atoi(q.Get("limit"))
        offset, _ := strconv.Atoi(q.Get("offset"))
        if limit <= 0 || limit > 200 {
                limit = 50
        }
        results, total, err := database.GetVanityResults(sessionID, status, search, limit, offset)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        jsonOK(w, map[string]interface{}{"results": results, "total": total, "limit": limit, "offset": offset})
}

func HandleVanityTagResult(w http.ResponseWriter, r *http.Request) {
        idStr := strings.TrimPrefix(r.URL.Path, "/api/vanity/results/tag/")
        id, err := strconv.ParseInt(idStr, 10, 64)
        if err != nil {
                jsonErr(w, "invalid id", http.StatusBadRequest)
                return
        }
        var body struct {
                Tags []string `json:"tags"`
        }
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
                jsonErr(w, err.Error(), http.StatusBadRequest)
                return
        }
        database.UpdateVanityResultTags(id, body.Tags)
        jsonOK(w, map[string]string{"status": "ok"})
}

func HandleClearVanityResults(w http.ResponseWriter, r *http.Request) {
        database.ClearVanityResults()
        globals.BroadcastLog("warn", "[Vanity] All results cleared")
        jsonOK(w, map[string]string{"status": "cleared"})
}

func HandleGetVanitySessions(w http.ResponseWriter, r *http.Request) {
        sessions, err := database.GetVanitySessions(50)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        jsonOK(w, sessions)
}

//  Proxies

func HandleGetProxies(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()
        limit, _ := strconv.Atoi(q.Get("limit"))
        offset, _ := strconv.Atoi(q.Get("offset"))
        if limit <= 0 || limit > 200 {
                limit = 50
        }
        if offset < 0 {
                offset = 0
        }
        proxies, total, err := database.GetProxiesPage(q.Get("filter"), limit, offset)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        jsonOK(w, map[string]interface{}{
                "proxies": proxies,
                "total":   total,
                "limit":   limit,
                "offset":  offset,
                "stats":   database.GetProxyStats(),
        })
}

func HandleAddProxy(w http.ResponseWriter, r *http.Request) {
        var body struct {
                URLs []string `json:"urls"`
                Type string   `json:"type"`
        }
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
                jsonErr(w, "invalid json", http.StatusBadRequest)
                return
        }
        added := 0
        for _, raw := range body.URLs {
                raw = strings.TrimSpace(raw)
                if raw == "" {
                        continue
                }
                pType := body.Type
                if pType == "" || pType == "auto" {
                        pType = proxy.DetectType(raw)
                }
                if !strings.HasPrefix(strings.ToLower(raw), "http") && !strings.HasPrefix(strings.ToLower(raw), "socks") {
                        raw = "http://" + raw
                }
                if _, err := database.SaveProxy(raw, pType); err == nil {
                        added++
                }
        }
        proxy.Default.Reload()
        globals.BroadcastLog("info", fmt.Sprintf("Added %d proxies", added))
        jsonOK(w, map[string]interface{}{"added": added})
}

func HandleFetchPublicProxies(w http.ResponseWriter, r *http.Request) {
        jsonErr(w, "public proxy auto-fetch is disabled", http.StatusGone)
}

func HandleDeleteProxy(w http.ResponseWriter, r *http.Request) {
        id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/proxies/"), 10, 64)
        if err != nil {
                jsonErr(w, "invalid id", http.StatusBadRequest)
                return
        }
        database.DeleteProxy(id)
        proxy.Default.Reload()
        jsonOK(w, map[string]string{"status": "deleted"})
}

func HandleDeleteAllProxies(w http.ResponseWriter, r *http.Request) {
        database.DeleteAllProxies()
        proxy.Default.Reload()
        globals.BroadcastLog("warn", "All proxies deleted")
        jsonOK(w, map[string]string{"status": "deleted"})
}

func HandleTestProxy(w http.ResponseWriter, r *http.Request) {
        id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/proxies/test/"), 10, 64)
        if err != nil {
                jsonErr(w, "invalid id", http.StatusBadRequest)
                return
        }
        p, err := database.GetProxyByID(id)
        if err != nil {
                jsonErr(w, "not found", http.StatusNotFound)
                return
        }
        latency, err := proxy.TestProxy(p, globals.UserAgent)
        if err != nil {
                database.UpdateProxyHealth(id, false, 0)
                proxy.Default.Reload()
                globals.BroadcastEvent("proxy_update", map[string]interface{}{"id": id, "healthy": false, "latency": 0})
                jsonOK(w, map[string]interface{}{"id": id, "healthy": false, "error": err.Error()})
                return
        }
        database.UpdateProxyHealth(id, true, latency)
        proxy.Default.Reload()
        globals.BroadcastEvent("proxy_update", map[string]interface{}{"id": id, "healthy": true, "latency": latency})
        jsonOK(w, map[string]interface{}{"id": id, "healthy": true, "latency": latency})
}

func HandleTestAllProxies(w http.ResponseWriter, r *http.Request) {
        proxies, _ := database.GetProxies()
        globals.BroadcastLog("info", fmt.Sprintf("Testing %d proxies...", len(proxies)))
        go func() {
                healthy, dead := 0, 0
                for i := range proxies {
                        p := proxies[i]
                        latency, err := proxy.TestProxy(&p, globals.UserAgent)
                        if err != nil {
                                dead++
                                database.UpdateProxyHealth(p.ID, false, 0)
                                globals.BroadcastEvent("proxy_update", map[string]interface{}{"id": p.ID, "healthy": false, "latency": 0})
                        } else {
                                healthy++
                                database.UpdateProxyHealth(p.ID, true, latency)
                                globals.BroadcastEvent("proxy_update", map[string]interface{}{"id": p.ID, "healthy": true, "latency": latency})
                        }
                }
                proxy.Default.Reload()
                globals.BroadcastLog("info", fmt.Sprintf("Proxy test done: %d healthy, %d dead", healthy, dead))
        }()
        jsonOK(w, map[string]string{"status": "testing"})
}

//  Token

func HandleTestToken(w http.ResponseWriter, r *http.Request) {
        var body struct {
                Token string `json:"token"`
        }
        if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
                jsonErr(w, "missing token", http.StatusBadRequest)
                return
        }

        req, err := http.NewRequestWithContext(r.Context(), http.MethodGet, globals.DiscordUsersMeAPI, nil)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        req.Header.Set("Authorization", body.Token)
        req.Header.Set("User-Agent", globals.UserAgent)
        req.Header.Set("Accept", "application/json")

        client := &http.Client{Timeout: 10 * time.Second}
        resp, err := client.Do(req)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusBadGateway)
                return
        }
        defer resp.Body.Close()

        if resp.StatusCode == 200 {
                var me struct {
                        Username      string `json:"username"`
                        Discriminator string `json:"discriminator"`
                        ID            string `json:"id"`
                }
                json.NewDecoder(resp.Body).Decode(&me)
                jsonOK(w, map[string]interface{}{
                        "valid":          true,
                        "username":       me.Username,
                        "discriminator":  me.Discriminator,
                        "id":             me.ID,
                })
                return
        }

        jsonOK(w, map[string]interface{}{
                "valid": false,
                "error": fmt.Sprintf("status %d", resp.StatusCode),
        })
}

//  Username Results

func HandleGetResults(w http.ResponseWriter, r *http.Request) {
        q := r.URL.Query()
        sessionID, _ := strconv.ParseInt(q.Get("session_id"), 10, 64)
        limit, _ := strconv.Atoi(q.Get("limit"))
        offset, _ := strconv.Atoi(q.Get("offset"))
        if limit <= 0 || limit > 200 {
                limit = 50
        }
        results, total, err := database.GetResults(sessionID, q.Get("status"), q.Get("search"), limit, offset)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        jsonOK(w, map[string]interface{}{"results": results, "total": total, "limit": limit, "offset": offset})
}

func HandleTagResult(w http.ResponseWriter, r *http.Request) {
        id, err := strconv.ParseInt(strings.TrimPrefix(r.URL.Path, "/api/results/tag/"), 10, 64)
        if err != nil {
                jsonErr(w, "invalid id", http.StatusBadRequest)
                return
        }
        var body struct {
                Tags []string `json:"tags"`
        }
        json.NewDecoder(r.Body).Decode(&body)
        database.UpdateResultTags(id, body.Tags)
        jsonOK(w, map[string]string{"status": "ok"})
}

func HandleClearResults(w http.ResponseWriter, r *http.Request) {
        database.ClearResults()
        globals.BroadcastLog("warn", "All username results cleared")
        jsonOK(w, map[string]string{"status": "cleared"})
}

func HandleGetSessions(w http.ResponseWriter, r *http.Request) {
        sessions, err := database.GetSessions(50)
        if err != nil {
                jsonErr(w, err.Error(), http.StatusInternalServerError)
                return
        }
        jsonOK(w, sessions)
}
