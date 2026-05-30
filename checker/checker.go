package checker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
	"users/database"
	"users/globals"
	"users/logger"
	"users/types"
)

var usernameErrorLogCount int64

func logUsernameCheckError(username string, err error, workerID int) {
	compact := globals.CompactCheckError(err)
	total, ok := globals.ShouldBroadcastCheckError(&usernameErrorLogCount)
	if !ok {
		return
	}
	msg := fmt.Sprintf("Check error [%s]: %s [T%d] (%d total)", username, compact, workerID, total)
	logger.Warn(msg)
	globals.BroadcastLog("warn", msg)
}

func CheckerInit(ctx context.Context, username string, sessionID int64, workerID int) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if CheckBlacklist(username) {
		atomic.AddInt64(&globals.InvalidUsernames, 1)
		msg := fmt.Sprintf("Username [%s] is blacklisted [T%d]", username, workerID)
		logger.Warn(msg)
		globals.BroadcastLog("warn", msg)

		if sessionID > 0 {
			r := &types.Result{
				SessionID: sessionID,
				Username:  username,
				Status:    "blacklisted",
				CheckedAt: time.Now(),
				Tags:      []string{},
				LatencyMs: 0,
			}
			database.SaveResult(r)
		}
		globals.BroadcastEvent("username_result", map[string]interface{}{
			"username": username,
			"status":   "blacklisted",
			"latency":  0,
		})
		return
	}

	if globals.Config.DryRun {
		atomic.AddInt64(&globals.ValidUsernames, 1)
		msg := fmt.Sprintf("[DRY RUN] Username [%s] would be checked [T%d]", username, workerID)
		logger.Info(msg)
		globals.BroadcastLog("info", msg)
		globals.BroadcastEvent("username_result", map[string]interface{}{
			"username": username,
			"status":   "dry_run",
			"latency":  0,
		})
		return
	}

	var taken bool
	var latency int
	var err error

	if globals.Config.DoubleVerify {
		taken, latency, err = CheckUsername(ctx, username)
	} else {
		taken, latency, err = CheckUsernameSimple(ctx, username)
	}

	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		atomic.AddInt64(&globals.InvalidUsernames, 1)
		logUsernameCheckError(username, err, workerID)

		if sessionID > 0 {
			database.SaveResult(&types.Result{
				SessionID: sessionID,
				Username:  username,
				Status:    "error",
				CheckedAt: time.Now(),
				Tags:      []string{},
				LatencyMs: latency,
			})
		}
		globals.BroadcastEvent("username_result", map[string]interface{}{
			"username": username,
			"status":   "error",
			"latency":  latency,
		})
		return
	}

	if taken {
		atomic.AddInt64(&globals.InvalidUsernames, 1)
		msg := fmt.Sprintf("Username [%s] is taken [T%d]", username, workerID)
		logger.Info(msg)
		globals.BroadcastLog("info", msg)

		globals.SaveBlackList(username)

		if sessionID > 0 {
			database.SaveResult(&types.Result{
				SessionID: sessionID,
				Username:  username,
				Status:    "taken",
				CheckedAt: time.Now(),
				Tags:      []string{},
				LatencyMs: latency,
			})
		}
		globals.BroadcastEvent("username_result", map[string]interface{}{
			"username": username,
			"status":   "taken",
			"latency":  latency,
		})
		return
	}

	atomic.AddInt64(&globals.ValidUsernames, 1)
	msg := fmt.Sprintf("Username [%s] is AVAILABLE! [T%d] (%dms)", username, workerID, latency)
	logger.Success(msg)
	globals.BroadcastLog("success", msg)

	globals.SaveBlackList(username)
	globals.SaveValidUser(username)

	if sessionID > 0 {
		database.SaveResult(&types.Result{
			SessionID: sessionID,
			Username:  username,
			Status:    "valid",
			CheckedAt: time.Now(),
			Tags:      []string{},
			LatencyMs: latency,
		})
	}

	globals.BroadcastEvent("username_result", map[string]interface{}{
		"username": username,
		"status":   "valid",
		"latency":  latency,
	})

	globals.SendDiscordWebhook(username)
}

func RunChecker(ctx context.Context, usernames []string, sessionID int64) {
	if !globals.IsCheckerRunning() {
		globals.SetCheckerRunning(true)
	}
	globals.CheckerStartTime = time.Now()
	atomic.StoreInt64(&globals.ValidUsernames, 0)
	atomic.StoreInt64(&globals.InvalidUsernames, 0)
	atomic.StoreInt64(&usernameErrorLogCount, 0)

	total := int64(len(usernames))
	logger.Info(fmt.Sprintf("Starting checker with %d usernames, %d threads", total, globals.Config.Threads))
	globals.BroadcastLog("info", fmt.Sprintf("Starting checker: %d usernames | %d threads", total, globals.Config.Threads))

	if globals.Config.DryRun {
		globals.BroadcastLog("warn", "DRY RUN mode enabled — no real requests will be made")
	}
	if globals.Config.DoubleVerify {
		globals.BroadcastLog("info", "Double verification mode enabled")
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				valid := atomic.LoadInt64(&globals.ValidUsernames)
				invalid := atomic.LoadInt64(&globals.InvalidUsernames)
				globals.BroadcastEvent("stats", types.StatsData{
					Valid:     valid,
					Invalid:   invalid,
					Total:     total,
					Remaining: total - valid - invalid,
					Rate:      globals.GetCurrentRate(),
					ElapsedMs: time.Since(globals.CheckerStartTime).Milliseconds(),
					Status:    globals.CheckerStatus(),
				})
				if sessionID > 0 {
					database.UpdateSessionStats(sessionID, int(valid), int(invalid))
				}
			}
		}
	}()

	usernameChannel := make(chan string, globals.Config.Threads*2)
	var wg sync.WaitGroup

	threads := globals.Config.Threads
	if threads < 1 {
		threads = 1
	}

	for i := 1; i <= threads; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case username, ok := <-usernameChannel:
					if !ok {
						return
					}
					CheckerInit(ctx, username, sessionID, workerID)
				}
			}
		}(i)
	}

	go func() {
		for _, username := range usernames {
			select {
			case <-ctx.Done():
				close(usernameChannel)
				return
			case usernameChannel <- username:
			}
		}
		close(usernameChannel)
	}()

	wg.Wait()

	valid := atomic.LoadInt64(&globals.ValidUsernames)
	invalid := atomic.LoadInt64(&globals.InvalidUsernames)

	status := "completed"
	select {
	case <-ctx.Done():
		status = "stopped"
	default:
	}

	if sessionID > 0 {
		database.UpdateSession(sessionID, int(valid), int(invalid), status)
	}

	globals.SetCheckerStopped()
	globals.BroadcastEvent("stats", types.StatsData{
		Valid:     valid,
		Invalid:   invalid,
		Total:     total,
		Remaining: total - valid - invalid,
		Rate:      0,
		ElapsedMs: time.Since(globals.CheckerStartTime).Milliseconds(),
		Status:    status,
	})
	globals.BroadcastEvent("checker_stopped", map[string]interface{}{
		"status":  status,
		"valid":   valid,
		"invalid": invalid,
	})

	msg := fmt.Sprintf("Checker %s. Valid: %d | Invalid: %d", status, valid, invalid)
	logger.Info(msg)
	globals.BroadcastLog("info", msg)
}
