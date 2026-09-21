package manager

import (
	"io"
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/knadh/listmonk/models"
)

// mockStore is a no-op manager.Store that records campaign status updates.
type mockStore struct {
	mu            sync.Mutex
	statusUpdates []statusUpdate
}

type statusUpdate struct {
	campID int
	status string
	reason string
}

func (s *mockStore) NextCampaigns(currentIDs []int64, sentCounts []int64) ([]*models.Campaign, error) {
	return nil, nil
}

func (s *mockStore) NextSubscribers(campID, limit int) ([]models.Subscriber, error) {
	return nil, nil
}

func (s *mockStore) GetCampaign(campID int) (*models.Campaign, error) {
	return &models.Campaign{Status: models.CampaignStatusRunning}, nil
}

func (s *mockStore) GetAttachment(mediaID int) (models.Attachment, error) {
	return models.Attachment{}, nil
}

func (s *mockStore) UpdateCampaignStatus(campID int, status string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statusUpdates = append(s.statusUpdates, statusUpdate{campID: campID, status: status, reason: reason})
	return nil
}

func (s *mockStore) UpdateCampaignCounts(campID int, toSend int, sent int, lastSubID int) error {
	return nil
}

func (s *mockStore) CreateLink(url string) (string, error) {
	return "", nil
}

func (s *mockStore) BlocklistSubscriber(id int64) error {
	return nil
}

func (s *mockStore) DeleteSubscriber(id int64) error {
	return nil
}

func (s *mockStore) lastStatusUpdate() (statusUpdate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.statusUpdates) == 0 {
		return statusUpdate{}, false
	}
	return s.statusUpdates[len(s.statusUpdates)-1], true
}

// mockMessenger is a no-op Messenger.
type mockMessenger struct{}

func (mockMessenger) Name() string              { return "email" }
func (mockMessenger) Push(models.Message) error { return nil }
func (mockMessenger) Flush() error              { return nil }
func (mockMessenger) Close() error              { return nil }

func newTestManager(cfg Config, store Store) *Manager {
	m := New(cfg, store, nil, log.New(io.Discard, "", 0))
	m.fnNotify = func(subject string, data any) error { return nil }
	return m
}

// TestSlidingWindowThrottle verifies that the window allows rate-1 messages
// immediately and throttles the message that hits the rate limit.
func TestSlidingWindowThrottle(t *testing.T) {
	sw := newSlidingWindow(3, time.Minute)
	now := time.Now()

	if wait := sw.throttle(now); wait != 0 {
		t.Fatalf("message 1 should not be throttled, got wait=%v", wait)
	}
	if wait := sw.throttle(now.Add(time.Second)); wait != 0 {
		t.Fatalf("message 2 should not be throttled, got wait=%v", wait)
	}

	// The 3rd message hits the limit and should wait out the remainder
	// of the window (~58s).
	wait := sw.throttle(now.Add(2 * time.Second))
	if wait <= 0 || wait > time.Minute {
		t.Fatalf("message 3 should be throttled with a sane wait, got %v", wait)
	}

	// After a throttle, the counter resets and the next message goes through.
	if wait := sw.throttle(now.Add(3 * time.Second)); wait != 0 {
		t.Fatalf("message after throttle should not be throttled, got wait=%v", wait)
	}
}

// TestSlidingWindowExpiry verifies that the counter resets once the window
// duration has elapsed.
func TestSlidingWindowExpiry(t *testing.T) {
	sw := newSlidingWindow(2, 10*time.Second)
	now := time.Now()

	if wait := sw.throttle(now); wait != 0 {
		t.Fatalf("message 1 should not be throttled, got wait=%v", wait)
	}

	// 11s later, the window has expired. The count resets instead of throttling.
	if wait := sw.throttle(now.Add(11 * time.Second)); wait != 0 {
		t.Fatalf("message after window expiry should not be throttled, got wait=%v", wait)
	}
}

// TestSlidingWindowConcurrency hammers the window from multiple goroutines
// (run with -race) and verifies that exactly total/rate throttles are
// triggered, ie: the check-and-increment is atomic and no message is
// over- or under-counted.
func TestSlidingWindowConcurrency(t *testing.T) {
	const (
		rate       = 100
		workers    = 8
		perWorker  = 100
		total      = workers * perWorker
		numWindows = total / rate
	)

	sw := newSlidingWindow(rate, time.Minute)
	now := time.Now()

	var (
		wg        sync.WaitGroup
		throttles atomic.Int64
	)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perWorker; j++ {
				if sw.throttle(now) > 0 {
					throttles.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if n := throttles.Load(); n != numWindows {
		t.Fatalf("expected %d throttles for %d messages at rate %d, got %d", numWindows, total, rate, n)
	}
}

// TestPipesIndependentSlidingWindows verifies that two pipes (campaigns)
// maintain independent windows and one campaign hitting its limit doesn't
// throttle the other.
func TestPipesIndependentSlidingWindows(t *testing.T) {
	p1 := &pipe{sliding: newSlidingWindow(2, time.Minute)}
	p2 := &pipe{sliding: newSlidingWindow(2, time.Minute)}
	now := time.Now()

	p1.sliding.throttle(now)
	if wait := p1.sliding.throttle(now); wait <= 0 {
		t.Fatal("pipe 1 should be throttled after hitting its rate limit")
	}

	// Pipe 2's window is unaffected by pipe 1's sends.
	if wait := p2.sliding.throttle(now); wait != 0 {
		t.Fatalf("pipe 2 should not be throttled by pipe 1's sends, got wait=%v", wait)
	}
}

// TestNewPipeAttachesPerPipeSlidingWindow verifies that newPipe attaches a
// distinct sliding window to every pipe when sliding window rate limiting
// is enabled.
func TestNewPipeAttachesPerPipeSlidingWindow(t *testing.T) {
	m := newTestManager(Config{
		Concurrency:           1,
		MessageRate:           1,
		BatchSize:             1,
		SlidingWindow:         true,
		SlidingWindowRate:     5,
		SlidingWindowDuration: time.Minute,
	}, &mockStore{})
	if err := m.AddMessenger(mockMessenger{}); err != nil {
		t.Fatalf("error adding messenger: %v", err)
	}

	p1, err := m.newPipe(&models.Campaign{ID: 1, Name: "camp1", Messenger: "email"})
	if err != nil {
		t.Fatalf("error creating pipe 1: %v", err)
	}
	p2, err := m.newPipe(&models.Campaign{ID: 2, Name: "camp2", Messenger: "email"})
	if err != nil {
		t.Fatalf("error creating pipe 2: %v", err)
	}

	if p1.sliding == nil || p2.sliding == nil {
		t.Fatal("pipes should have sliding windows attached when enabled")
	}
	if p1.sliding == p2.sliding {
		t.Fatal("each pipe should have its own sliding window instance")
	}

	// Stop and release the pipes so their cleanup goroutines exit.
	for _, p := range []*pipe{p1, p2} {
		p.Stop(false)
		p.wg.Done()
	}
}

// TestNewPipeNoSlidingWindowWhenDisabled verifies that no window is attached
// when sliding window rate limiting is disabled.
func TestNewPipeNoSlidingWindowWhenDisabled(t *testing.T) {
	m := newTestManager(Config{
		Concurrency: 1,
		MessageRate: 1,
		BatchSize:   1,
	}, &mockStore{})
	if err := m.AddMessenger(mockMessenger{}); err != nil {
		t.Fatalf("error adding messenger: %v", err)
	}

	p, err := m.newPipe(&models.Campaign{ID: 1, Name: "camp1", Messenger: "email"})
	if err != nil {
		t.Fatalf("error creating pipe: %v", err)
	}
	if p.sliding != nil {
		t.Fatal("pipe should not have a sliding window when disabled")
	}

	p.Stop(false)
	p.wg.Done()
}

// TestCleanupRecordsPauseReason verifies that when a campaign is auto-paused
// after exceeding the error threshold, the pause reason is persisted via
// UpdateCampaignStatus.
func TestCleanupRecordsPauseReason(t *testing.T) {
	store := &mockStore{}
	m := newTestManager(Config{
		Concurrency:   1,
		MessageRate:   1,
		BatchSize:     1,
		MaxSendErrors: 1,
	}, store)

	p := &pipe{
		camp: &models.Campaign{ID: 1, Name: "camp1"},
		wg:   &sync.WaitGroup{},
		m:    m,
	}

	// Simulate the error threshold being hit, which auto-stops the pipe.
	p.OnError()
	p.cleanup()

	up, ok := store.lastStatusUpdate()
	if !ok {
		t.Fatal("expected a campaign status update")
	}
	if up.status != models.CampaignStatusPaused {
		t.Fatalf("expected campaign to be paused, got %s", up.status)
	}
	if up.reason == "" {
		t.Fatal("expected a non-empty pause reason to be recorded")
	}
}

// TestCleanupFinishedClearsPauseReason verifies that a naturally finished
// campaign is marked finished with an empty reason (clearing any stale
// pause reason in the DB).
func TestCleanupFinishedClearsPauseReason(t *testing.T) {
	store := &mockStore{}
	m := newTestManager(Config{
		Concurrency: 1,
		MessageRate: 1,
		BatchSize:   1,
	}, store)

	p := &pipe{
		camp: &models.Campaign{ID: 1, Name: "camp1"},
		wg:   &sync.WaitGroup{},
		m:    m,
	}
	p.cleanup()

	up, ok := store.lastStatusUpdate()
	if !ok {
		t.Fatal("expected a campaign status update")
	}
	if up.status != models.CampaignStatusFinished {
		t.Fatalf("expected campaign to be finished, got %s", up.status)
	}
	if up.reason != "" {
		t.Fatalf("expected empty pause reason, got %q", up.reason)
	}
}
