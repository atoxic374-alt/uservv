package globals

import (
	"context"
	"testing"
	"time"
)

func resetTestLimiter(route string) {
	l := limiterFor(route)
	l.mu.Lock()
	l.rateDelay = 0
	l.until = time.Time{}
	l.lastLog = time.Time{}
	l.lastCall = time.Time{}
	l.mu.Unlock()
}

func TestCooldownRemainingDoesNotBlockWhileSlotWaits(t *testing.T) {
	resetTestLimiter(RateRouteUsername)
	defer resetTestLimiter(RateRouteUsername)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	RegisterDiscordRateLimitFor(RateRouteUsername, "3")
	started := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		close(started)
		done <- WaitForDiscordSlotFor(ctx, RateRouteUsername, 0)
	}()
	<-started

	time.Sleep(25 * time.Millisecond)
	readDone := make(chan time.Duration, 1)
	go func() {
		readDone <- CooldownRemaining(RateRouteUsername)
	}()

	select {
	case remaining := <-readDone:
		if remaining <= 0 {
			t.Fatalf("expected active cooldown, got %s", remaining)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("CooldownRemaining blocked while WaitForDiscordSlotFor was waiting")
	}

	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDiscordSlotFor did not return after cancellation")
	}
}

func TestWaitForDiscordSlotHonorsCancellation(t *testing.T) {
	resetTestLimiter(RateRouteVanity)
	defer resetTestLimiter(RateRouteVanity)

	ctx, cancel := context.WithCancel(context.Background())
	RegisterDiscordRateLimitFor(RateRouteVanity, "3")

	done := make(chan error, 1)
	go func() {
		done <- WaitForDiscordSlotFor(ctx, RateRouteVanity, 0)
	}()

	time.Sleep(25 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancellation error")
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForDiscordSlotFor did not return after cancellation")
	}
}
