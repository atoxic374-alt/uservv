package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
	"users/types"

	_ "modernc.org/sqlite"
)

var DB *sql.DB

func Init(path string) error {
	var err error
	DB, err = sql.Open("sqlite", path)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	DB.SetMaxOpenConns(1)
	if err := DB.Ping(); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}
	return createTables()
}

func createTables() error {
	schema := `
	PRAGMA journal_mode=WAL;

	CREATE TABLE IF NOT EXISTS sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		started_at DATETIME NOT NULL,
		finished_at DATETIME,
		total INTEGER DEFAULT 0,
		valid INTEGER DEFAULT 0,
		invalid INTEGER DEFAULT 0,
		mode TEXT DEFAULT 'random',
		status TEXT DEFAULT 'running'
	);

	CREATE TABLE IF NOT EXISTS results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		session_id INTEGER NOT NULL,
		username TEXT NOT NULL,
		status TEXT NOT NULL,
		checked_at DATETIME NOT NULL,
		tags TEXT DEFAULT '[]',
		latency_ms INTEGER DEFAULT 0
	);
	CREATE INDEX IF NOT EXISTS idx_results_username ON results(username);
	CREATE INDEX IF NOT EXISTS idx_results_status ON results(status);
	CREATE INDEX IF NOT EXISTS idx_results_session ON results(session_id);

	CREATE TABLE IF NOT EXISTS proxies (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		url TEXT UNIQUE NOT NULL,
		type TEXT DEFAULT 'http',
		healthy INTEGER DEFAULT 1,
		last_checked DATETIME,
		success_count INTEGER DEFAULT 0,
		fail_count INTEGER DEFAULT 0,
		avg_latency_ms INTEGER DEFAULT 0,
		total_latency INTEGER DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS vanity_sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		started_at DATETIME NOT NULL,
		finished_at DATETIME,
		total INTEGER DEFAULT 0,
		available INTEGER DEFAULT 0,
		taken INTEGER DEFAULT 0,
		status TEXT DEFAULT 'running',
		config TEXT DEFAULT '{}'
	);

	CREATE TABLE IF NOT EXISTS vanity_results (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		session_id INTEGER NOT NULL,
		code TEXT NOT NULL,
		status TEXT NOT NULL,
		guild_name TEXT DEFAULT '',
		checked_at DATETIME NOT NULL,
		tags TEXT DEFAULT '[]',
		latency_ms INTEGER DEFAULT 0
	);
	CREATE UNIQUE INDEX IF NOT EXISTS idx_vanity_code ON vanity_results(code);
	CREATE INDEX IF NOT EXISTS idx_vanity_status ON vanity_results(status);
	CREATE INDEX IF NOT EXISTS idx_vanity_session ON vanity_results(session_id);
	`
	_, err := DB.Exec(schema)
	return err
}

//  Username sessions

func CreateSession(mode string, total int) (int64, error) {
	res, err := DB.Exec(`INSERT INTO sessions (started_at, total, mode, status) VALUES (?, ?, ?, 'running')`,
		time.Now(), total, mode)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func UpdateSession(id int64, valid, invalid int, status string) error {
	_, err := DB.Exec(`UPDATE sessions SET valid=?, invalid=?, status=?, finished_at=? WHERE id=?`,
		valid, invalid, status, time.Now(), id)
	return err
}

func UpdateSessionStats(id int64, valid, invalid int) error {
	_, err := DB.Exec(`UPDATE sessions SET valid=?, invalid=? WHERE id=?`, valid, invalid, id)
	return err
}

func GetSessions(limit int) ([]types.Session, error) {
	rows, err := DB.Query(`SELECT id, started_at, finished_at, total, valid, invalid, mode, status FROM sessions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []types.Session
	for rows.Next() {
		var s types.Session
		var fa sql.NullTime
		if err := rows.Scan(&s.ID, &s.StartedAt, &fa, &s.Total, &s.Valid, &s.Invalid, &s.Mode, &s.Status); err != nil {
			return nil, err
		}
		if fa.Valid {
			s.FinishedAt = &fa.Time
		}
		sessions = append(sessions, s)
	}
	if sessions == nil {
		sessions = []types.Session{}
	}
	return sessions, nil
}

//  Username results

func SaveResult(r *types.Result) (int64, error) {
	tagsJSON, _ := json.Marshal(r.Tags)
	res, err := DB.Exec(`INSERT INTO results (session_id, username, status, checked_at, tags, latency_ms) VALUES (?, ?, ?, ?, ?, ?)`,
		r.SessionID, r.Username, r.Status, r.CheckedAt, string(tagsJSON), r.LatencyMs)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func GetResults(sessionID int64, status string, search string, limit, offset int) ([]types.Result, int, error) {
	where := " WHERE 1=1"
	args := []interface{}{}
	if sessionID > 0 {
		where += " AND session_id=?"
		args = append(args, sessionID)
	}
	if status != "" {
		where += " AND status=?"
		args = append(args, status)
	}
	if search != "" {
		where += " AND username LIKE ?"
		args = append(args, "%"+search+"%")
	}
	var total int
	ca := make([]interface{}, len(args))
	copy(ca, args)
	DB.QueryRow("SELECT COUNT(*) FROM results"+where, ca...).Scan(&total)
	rows, err := DB.Query("SELECT id, session_id, username, status, checked_at, tags, latency_ms FROM results"+where+" ORDER BY id DESC LIMIT ? OFFSET ?", append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var results []types.Result
	for rows.Next() {
		var r types.Result
		var tagsJSON string
		if err := rows.Scan(&r.ID, &r.SessionID, &r.Username, &r.Status, &r.CheckedAt, &tagsJSON, &r.LatencyMs); err != nil {
			return nil, 0, err
		}
		json.Unmarshal([]byte(tagsJSON), &r.Tags)
		if r.Tags == nil {
			r.Tags = []string{}
		}
		results = append(results, r)
	}
	if results == nil {
		results = []types.Result{}
	}
	return results, total, nil
}

func UpdateResultTags(id int64, tags []string) error {
	tagsJSON, _ := json.Marshal(tags)
	_, err := DB.Exec(`UPDATE results SET tags=? WHERE id=?`, string(tagsJSON), id)
	return err
}

func GetCheckedUsernames() (map[string]bool, error) {
	rows, err := DB.Query(`SELECT DISTINCT username FROM results`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]bool)
	for rows.Next() {
		var u string
		rows.Scan(&u)
		m[u] = true
	}
	return m, nil
}

func ClearResults() error {
	_, err := DB.Exec(`DELETE FROM results`)
	return err
}

//  Proxies

func SaveProxy(rawURL, proxyType string) (int64, error) {
	res, err := DB.Exec(`INSERT OR IGNORE INTO proxies (url, type, healthy, last_checked) VALUES (?, ?, 1, ?)`,
		rawURL, proxyType, time.Now())
	if err != nil {
		return 0, err
	}
	id, _ := res.LastInsertId()
	return id, nil
}

func GetProxies() ([]types.Proxy, error) {
	rows, err := DB.Query(`SELECT id, url, type, healthy, last_checked, success_count, fail_count, avg_latency_ms FROM proxies ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var proxies []types.Proxy
	for rows.Next() {
		var p types.Proxy
		var lc sql.NullTime
		var healthy int
		rows.Scan(&p.ID, &p.URL, &p.Type, &healthy, &lc, &p.SuccessCount, &p.FailCount, &p.AvgLatencyMs)
		p.Healthy = healthy == 1
		if lc.Valid {
			p.LastChecked = lc.Time
		}
		proxies = append(proxies, p)
	}
	if proxies == nil {
		proxies = []types.Proxy{}
	}
	return proxies, nil
}

func scanProxies(rows *sql.Rows) ([]types.Proxy, error) {
	defer rows.Close()
	var proxies []types.Proxy
	for rows.Next() {
		var p types.Proxy
		var lc sql.NullTime
		var healthy int
		if err := rows.Scan(&p.ID, &p.URL, &p.Type, &healthy, &lc, &p.SuccessCount, &p.FailCount, &p.AvgLatencyMs); err != nil {
			return nil, err
		}
		p.Healthy = healthy == 1
		if lc.Valid {
			p.LastChecked = lc.Time
		}
		proxies = append(proxies, p)
	}
	if proxies == nil {
		proxies = []types.Proxy{}
	}
	return proxies, rows.Err()
}

func proxyWhere(filter string) string {
	switch filter {
	case "healthy":
		return "WHERE healthy=1"
	case "dead":
		return "WHERE healthy=0"
	default:
		return ""
	}
}

func GetProxiesPage(filter string, limit, offset int) ([]types.Proxy, int, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	if offset < 0 {
		offset = 0
	}
	where := proxyWhere(filter)
	var total int
	if err := DB.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM proxies %s`, where)).Scan(&total); err != nil {
		return nil, 0, err
	}
	rows, err := DB.Query(fmt.Sprintf(`SELECT id, url, type, healthy, last_checked, success_count, fail_count, avg_latency_ms FROM proxies %s ORDER BY id DESC LIMIT ? OFFSET ?`, where), limit, offset)
	if err != nil {
		return nil, 0, err
	}
	proxies, err := scanProxies(rows)
	if err != nil {
		return nil, 0, err
	}
	return proxies, total, nil
}

func GetProxyByID(id int64) (*types.Proxy, error) {
	row := DB.QueryRow(`SELECT id, url, type, healthy, last_checked, success_count, fail_count, avg_latency_ms FROM proxies WHERE id=?`, id)
	var p types.Proxy
	var lc sql.NullTime
	var healthy int
	if err := row.Scan(&p.ID, &p.URL, &p.Type, &healthy, &lc, &p.SuccessCount, &p.FailCount, &p.AvgLatencyMs); err != nil {
		return nil, err
	}
	p.Healthy = healthy == 1
	if lc.Valid {
		p.LastChecked = lc.Time
	}
	return &p, nil
}

func GetProxyStats() map[string]interface{} {
	stats := map[string]interface{}{}
	var total, healthy int
	var avgLatency sql.NullFloat64
	DB.QueryRow(`SELECT COUNT(*) FROM proxies`).Scan(&total)
	DB.QueryRow(`SELECT COUNT(*) FROM proxies WHERE healthy=1`).Scan(&healthy)
	DB.QueryRow(`SELECT COALESCE(AVG(CASE WHEN healthy=1 AND avg_latency_ms>0 THEN avg_latency_ms END), 0) FROM proxies`).Scan(&avgLatency)
	stats["total"] = total
	stats["healthy"] = healthy
	stats["dead"] = total - healthy
	if avgLatency.Valid {
		stats["avg_latency_ms"] = int(avgLatency.Float64)
	} else {
		stats["avg_latency_ms"] = 0
	}
	return stats
}

func GetHealthyProxies() ([]types.Proxy, error) {
	rows, err := DB.Query(`SELECT id, url, type, healthy, last_checked, success_count, fail_count, avg_latency_ms FROM proxies WHERE healthy=1 ORDER BY avg_latency_ms ASC, success_count DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var proxies []types.Proxy
	for rows.Next() {
		var p types.Proxy
		var lc sql.NullTime
		var healthy int
		rows.Scan(&p.ID, &p.URL, &p.Type, &healthy, &lc, &p.SuccessCount, &p.FailCount, &p.AvgLatencyMs)
		p.Healthy = healthy == 1
		if lc.Valid {
			p.LastChecked = lc.Time
		}
		proxies = append(proxies, p)
	}
	return proxies, nil
}

func UpdateProxyHealth(id int64, success bool, latencyMs int) error {
	if success {
		_, err := DB.Exec(`UPDATE proxies SET healthy=1, last_checked=?, success_count=success_count+1, total_latency=total_latency+?, avg_latency_ms=CASE WHEN success_count=0 THEN ? ELSE (total_latency+?)/(success_count+1) END WHERE id=?`,
			time.Now(), latencyMs, latencyMs, latencyMs, id)
		return err
	}
	_, err := DB.Exec(`UPDATE proxies SET healthy=0, last_checked=?, fail_count=fail_count+1 WHERE id=?`, time.Now(), id)
	return err
}

func DeleteProxy(id int64) error {
	_, err := DB.Exec(`DELETE FROM proxies WHERE id=?`, id)
	return err
}

func DeleteAllProxies() error {
	_, err := DB.Exec(`DELETE FROM proxies`)
	return err
}

//  Vanity sessions

func CreateVanitySession(total int, configJSON string) (int64, error) {
	res, err := DB.Exec(`INSERT INTO vanity_sessions (started_at, total, status, config) VALUES (?, ?, 'running', ?)`,
		time.Now(), total, configJSON)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func UpdateVanitySession(id int64, available, taken int, status string) error {
	if status == "running" {
		_, err := DB.Exec(`UPDATE vanity_sessions SET available=?, taken=? WHERE id=?`, available, taken, id)
		return err
	}
	_, err := DB.Exec(`UPDATE vanity_sessions SET available=?, taken=?, status=?, finished_at=? WHERE id=?`,
		available, taken, status, time.Now(), id)
	return err
}

func GetVanitySessions(limit int) ([]types.VanitySession, error) {
	rows, err := DB.Query(`SELECT id, started_at, finished_at, total, available, taken, status, config FROM vanity_sessions ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var sessions []types.VanitySession
	for rows.Next() {
		var s types.VanitySession
		var fa sql.NullTime
		rows.Scan(&s.ID, &s.StartedAt, &fa, &s.Total, &s.Available, &s.Taken, &s.Status, &s.Config)
		if fa.Valid {
			s.FinishedAt = &fa.Time
		}
		sessions = append(sessions, s)
	}
	if sessions == nil {
		sessions = []types.VanitySession{}
	}
	return sessions, nil
}

//  Vanity results

func SaveVanityResult(r *types.VanityResult) (int64, error) {
	tagsJSON, _ := json.Marshal(r.Tags)
	res, err := DB.Exec(`INSERT OR IGNORE INTO vanity_results (session_id, code, status, guild_name, checked_at, tags, latency_ms) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		r.SessionID, r.Code, r.Status, r.GuildName, r.CheckedAt, string(tagsJSON), r.LatencyMs)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func GetVanityResults(sessionID int64, status string, search string, limit, offset int) ([]types.VanityResult, int, error) {
	where := " WHERE 1=1"
	args := []interface{}{}
	if sessionID > 0 {
		where += " AND session_id=?"
		args = append(args, sessionID)
	}
	if status != "" {
		where += " AND status=?"
		args = append(args, status)
	}
	if search != "" {
		where += " AND code LIKE ?"
		args = append(args, "%"+search+"%")
	}
	var total int
	ca := make([]interface{}, len(args))
	copy(ca, args)
	DB.QueryRow("SELECT COUNT(*) FROM vanity_results"+where, ca...).Scan(&total)
	rows, err := DB.Query("SELECT id, session_id, code, status, guild_name, checked_at, tags, latency_ms FROM vanity_results"+where+" ORDER BY id DESC LIMIT ? OFFSET ?", append(args, limit, offset)...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var results []types.VanityResult
	for rows.Next() {
		var r types.VanityResult
		var tagsJSON string
		rows.Scan(&r.ID, &r.SessionID, &r.Code, &r.Status, &r.GuildName, &r.CheckedAt, &tagsJSON, &r.LatencyMs)
		json.Unmarshal([]byte(tagsJSON), &r.Tags)
		if r.Tags == nil {
			r.Tags = []string{}
		}
		results = append(results, r)
	}
	if results == nil {
		results = []types.VanityResult{}
	}
	return results, total, nil
}

func GetCheckedVanityCodes() (map[string]bool, error) {
	rows, err := DB.Query(`SELECT DISTINCT code FROM vanity_results`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	m := make(map[string]bool)
	for rows.Next() {
		var c string
		rows.Scan(&c)
		m[c] = true
	}
	return m, nil
}

func UpdateVanityResultTags(id int64, tags []string) error {
	tagsJSON, _ := json.Marshal(tags)
	_, err := DB.Exec(`UPDATE vanity_results SET tags=? WHERE id=?`, string(tagsJSON), id)
	return err
}

func ClearVanityResults() error {
	_, err := DB.Exec(`DELETE FROM vanity_results`)
	return err
}

//  Stats

func GetDBStats() map[string]interface{} {
	stats := map[string]interface{}{}
	var tr, vr, ts, tp, hp, tvr, avr int
	DB.QueryRow(`SELECT COUNT(*) FROM results`).Scan(&tr)
	DB.QueryRow(`SELECT COUNT(*) FROM results WHERE status='valid'`).Scan(&vr)
	DB.QueryRow(`SELECT COUNT(*) FROM sessions`).Scan(&ts)
	DB.QueryRow(`SELECT COUNT(*) FROM proxies`).Scan(&tp)
	DB.QueryRow(`SELECT COUNT(*) FROM proxies WHERE healthy=1`).Scan(&hp)
	DB.QueryRow(`SELECT COUNT(*) FROM vanity_results`).Scan(&tvr)
	DB.QueryRow(`SELECT COUNT(*) FROM vanity_results WHERE status='available'`).Scan(&avr)
	stats["total_results"] = tr
	stats["valid_results"] = vr
	stats["total_sessions"] = ts
	stats["total_proxies"] = tp
	stats["healthy_proxies"] = hp
	stats["total_vanity"] = tvr
	stats["available_vanity"] = avr
	return stats
}
