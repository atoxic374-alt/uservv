---
name: SQLite on Replit
description: Which SQLite driver to use in Go projects on Replit
---

Use `modernc.org/sqlite` — pure Go implementation, no CGO needed.

**Why:** `mattn/go-sqlite3` requires CGO (`cgo enabled`) which is unreliable on Replit's NixOS environment and produces "exec format error" or build failures.

**How to apply:** In go.mod, add `modernc.org/sqlite`. Import with `_ "modernc.org/sqlite"` and open with `sql.Open("sqlite", path)`. Set `DB.SetMaxOpenConns(1)` and `PRAGMA journal_mode=WAL` for concurrent access.
