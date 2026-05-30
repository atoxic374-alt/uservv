---
name: Vanity URL check API
description: How Discord vanity URL availability is checked and deduplicated
---

Endpoint: `GET https://discord.com/api/v9/invites/{code}?with_counts=false`

- HTTP 404 → code is **available** (no invite exists)
- HTTP 200 → code is **taken** (returns JSON with guild/invite info)
- HTTP 429 → rate limited (increase adaptive delay)

**Why this endpoint:** Vanity URLs are just special Discord invites — the public invite lookup API returns 404 for non-existent codes including vacant vanity slots.

**Persistent deduplication:** `vanity_results` table has `UNIQUE INDEX` on `code` column + `INSERT OR IGNORE`. `GetCheckedVanityCodes()` returns all ever-checked codes. The checker filters these out before starting — so codes are never re-checked across sessions.

**How to apply:** Any new vanity checker must call `database.GetCheckedVanityCodes()` at start (when `skip_duplicates=true`) and filter the generated code list before running goroutines.

**Generation modes:**
- Random: generates N random codes with given charset and length range
- Exhaustive: all combinations for lengths [min, max], capped at 500,000
- Custom: reads from `data/vanity.txt`, one code per line
