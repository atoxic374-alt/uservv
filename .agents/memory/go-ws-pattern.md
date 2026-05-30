---
name: Go WebSocket broadcast pattern
description: How real-time events flow from checker goroutines to browser clients
---

Pattern used in this project for real-time updates:

1. `globals.EventCh` (buffered chan types.Event, size 512) — any goroutine writes events here
2. `globals.BroadcastEvent(type, data)` — helper that writes to EventCh without blocking
3. In `web/server.go`: a goroutine reads EventCh and calls `WSHub.Broadcast(event)`
4. `WSHub` (gorilla/websocket hub) fans out to all connected clients via per-client `send` channels

**Why:** Decouples business logic from WebSocket layer. Checker goroutines don't need to know about WebSocket.

**How to apply:** Always use `globals.BroadcastEvent` from business logic. Never import `web` package from `checker` or `globals`.
