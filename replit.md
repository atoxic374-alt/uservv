# Discord Checker

A full-stack web dashboard for bulk Discord username availability checking AND vanity URL checking, with multi-threading, proxy rotation, SQLite persistence, and real-time WebSocket updates.

## Tech Stack
- **Language**: Go 1.24+
- **Database**: SQLite via `modernc.org/sqlite` (pure Go, no CGO)
- **WebSocket**: `gorilla/websocket` for real-time updates
- **Proxy**: `golang.org/x/net/proxy` for SOCKS5 support
- **Frontend**: Vanilla HTML/CSS/JS single-page app (dark theme, responsive)
- **Port**: 5000 (web dashboard)

## Running
```bash
go run .
```
Then open the preview pane on port 5000.

## Project Structure
```
main.go              — Entry point, starts web server on 0.0.0.0:5000
checker/
  checker.go         — Username checker (concurrent goroutines, context-aware stop)
  helper.go          — HTTP requests with proxy support and retry
vanity/
  checker.go         — Vanity URL checker (discord.gg/{code}), code generator
database/
  db.go              — SQLite: sessions, results, proxies, vanity tables
proxy/
  manager.go         — Proxy pool, health checks, SOCKS5 support
globals/
  globals.go         — Shared state, config I/O, event broadcasting
web/
  server.go          — HTTP routes
  handlers.go        — REST API handlers (username + vanity)
  ws.go              — WebSocket hub (real-time push)
  static/index.html  — Full SPA dashboard (responsive mobile+PC)
types/types.go       — Shared type definitions
data/
  config.json        — Username checker config (auto-saved)
  vanity_config.json — Vanity checker config (auto-saved)
  checker.db         — SQLite database (auto-created on first run)
  usernames.txt      — Custom username list
  vanity.txt         — Custom vanity code list
  proxies.txt        — Legacy proxy list (database is primary)
  blacklist.txt      — Already-checked usernames
  valids.txt         — Text backup of found valid usernames
```

## Dashboard Sections
1. **Dashboard** — Live stats (username + vanity), dual progress bars, activity log, DB stats
2. **Username Checker** — Start/stop, toggles (dry run, double verify, adaptive rate, skip dups)
3. **Username Results** — Filter/search checked usernames, add tags, pagination
4. **Username Sessions** — History of username checking sessions
5. **Vanity Checker** — Check discord.gg/{code} availability with:
   - 3 generation modes: Random / Exhaustive / Custom List
   - Length range control (min/max sliders, 2–12 chars)
   - Prefix filter (codes must start with X)
   - Charset: letters only / alphanumeric / custom chars
   - Combination count estimate
   - Persistent deduplication (checked codes never re-checked across sessions)
6. **Vanity Results** — Filter available/taken, add tags, copy discord.gg links
7. **Vanity Sessions** — History of vanity checking sessions
8. **Proxy Manager** — Add HTTP/SOCKS5, health check, latency stats
9. **Settings** — All username checker configuration

## Vanity URL API
`GET https://discord.com/api/v9/invites/{code}?with_counts=false`
- 200 → taken (returns guild info)
- 404 → available
- 429 → rate limited

## User Preferences
- Arabic is acceptable for communication
