package manager

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/knadh/listmonk/models"
	"github.com/paulbellamy/ratecounter"
)

// pauseReasonTooManyErrors is recorded as the pause reason when a campaign
// is auto-paused after exceeding the send error threshold.
const pauseReasonTooManyErrors = "Too many send errors"

// slidingWindow implements a per-campaign (per-pipe) rate limiting window.
// It keeps track of the number of messages sent in a period and on reaching
// the specified limit, asks the caller to wait until the window is over
// before sending further messages. The counter and the window start are
// guarded by a mutex so that concurrent senders observe a consistent state.
type slidingWindow struct {
	mu     sync.Mutex
	count  int
	start  time.Time
	rate   int
	window time.Duration
}

// newSlidingWindow returns a slidingWindow that allows rate messages per window.
func newSlidingWindow(rate int, window time.Duration) *slidingWindow {
	return &slidingWindow{
		start:  time.Now(),
		rate:   rate,
		window: window,
	}
}

// throttle atomically registers one message in the window and returns the
// duration the caller should wait before proceeding. It returns 0 if the
// message can be sent immediately. The check-and-increment is performed
// under a lock so concurrent callers can't over-shoot the window's rate.
func (s *slidingWindow) throttle(now time.Time) time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()

	diff := now.Sub(s.start)

	// Window has expired. Reset the clock.
	if diff >= s.window {
		s.start = now
		s.count = 0
		diff = 0
	}

	// Have the messages exceeded the limit?
	s.count++
	if s.count >= s.rate {
		s.count = 0
		return s.window - diff
	}

	return 0
}

type pipe struct {
	camp       *models.Campaign
	rate       *ratecounter.RateCounter
	wg         *sync.WaitGroup
	sent       atomic.Int64
	lastID     atomic.Uint64
	errors     atomic.Uint64
	stopped    atomic.Bool
	withErrors atomic.Bool

	// Per-campaign sliding window rate limiter. nil if disabled.
	sliding *slidingWindow

	// Reason recorded when the campaign is auto-paused (eg: too many errors).
	pauseReason atomic.Value

	m *Manager
}

// newPipe adds a campaign to the process queue.
func (m *Manager) newPipe(c *models.Campaign) (*pipe, error) {
	// Validate messenger.
	if _, ok := m.messengers[c.Messenger]; !ok {
		m.store.UpdateCampaignStatus(c.ID, models.CampaignStatusCancelled, "")
		return nil, fmt.Errorf("unknown messenger %s on campaign %s", c.Messenger, c.Name)
	}

	// Load the template.
	if err := c.CompileTemplate(m.TemplateFuncs(c)); err != nil {
		return nil, err
	}

	// Load any media/attachments.
	if err := m.attachMedia(c); err != nil {
		return nil, err
	}

	// Add the campaign to the active map.
	p := &pipe{
		camp: c,
		rate: ratecounter.NewRateCounter(time.Minute),
		wg:   &sync.WaitGroup{},
		m:    m,
	}

	// Attach a per-campaign sliding window rate limiter if it's enabled.
	// Every pipe gets its own independent window so that one campaign's
	// send rate doesn't throttle other concurrently running campaigns.
	if m.cfg.SlidingWindow &&
		m.cfg.SlidingWindowRate > 0 &&
		m.cfg.SlidingWindowDuration.Seconds() > 1 {
		p.sliding = newSlidingWindow(m.cfg.SlidingWindowRate, m.cfg.SlidingWindowDuration)
	}

	// Increment the waitgroup so that Wait() blocks immediately. This is necessary
	// as a campaign pipe is created first and subscribers/messages under it are
	// fetched asynchronolusly later. The messages each add to the wg and that
	// count is used to determine the exhaustion/completion of all messages.
	p.wg.Add(1)

	go func() {
		// Wait for all the messages in the campaign to be processed
		// (successfully or skipped after errors or cancellation).
		p.wg.Wait()

		p.cleanup()
	}()

	m.pipesMut.Lock()
	m.pipes[c.ID] = p
	m.pipesMut.Unlock()
	return p, nil
}

// NextSubscribers processes the next batch of subscribers in a given campaign.
// It returns a bool indicating whether any subscribers were processed
// in the current batch or not. A false indicates that all subscribers
// have been processed, or that a campaign has been paused or cancelled.
func (p *pipe) NextSubscribers() (bool, error) {
	// Fetch the next batch of subscribers from a 'running' campaign.
	subs, err := p.m.store.NextSubscribers(p.camp.ID, p.m.cfg.BatchSize)
	if err != nil {
		return false, fmt.Errorf("error fetching campaign subscribers (%s): %v", p.camp.Name, err)
	}

	// There are no subscribers from the query. Either all subscribers on the campaign
	// have been processed, or the campaign has changed from 'running' to 'paused' or 'cancelled'.
	if len(subs) == 0 {
		return false, nil
	}

	// Push messages.
	for _, s := range subs {
		msg, err := p.newMessage(s)
		if err != nil {
			p.m.log.Printf("error rendering message (%s) (%s): %v", p.camp.Name, s.Email, err)
			continue
		}

		// Push the message to the queue while blocking and waiting until
		// the queue is drained.
		p.m.campMsgQ <- msg

		// Check if this campaign's sliding window is active. The window
		// check-and-increment is atomic; only the sleep happens outside
		// the lock so other pipes aren't blocked while waiting.
		if p.sliding != nil {
			if wait := p.sliding.throttle(time.Now()); wait > 0 {
				p.m.log.Printf("campaign (%s) exceeded %d messages for the window (%v). Sleeping for %s.",
					p.camp.Name,
					p.m.cfg.SlidingWindowRate,
					p.m.cfg.SlidingWindowDuration,
					wait.Round(time.Second))

				time.Sleep(wait)
			}
		}
	}

	return true, nil
}

// OnError keeps track of the number of errors that occur while sending messages
// and pauses the campaign if the error threshold is met.
func (p *pipe) OnError() {
	if p.m.cfg.MaxSendErrors < 1 {
		return
	}

	// If the error threshold is met, pause the campaign.
	count := p.errors.Add(1)
	if int(count) < p.m.cfg.MaxSendErrors {
		return
	}

	p.Stop(true)
	p.m.log.Printf("error count exceeded %d. pausing campaign %s", p.m.cfg.MaxSendErrors, p.camp.Name)
}

// Stop "marks" a campaign as stopped. It doesn't actually stop the processing
// of messages. That happens when every queued message in the campaign is processed,
// marking .wg, the waitgroup counter as done. That triggers cleanup().
func (p *pipe) Stop(withErrors bool) {
	// Already stopped.
	if p.stopped.Load() {
		return
	}

	if withErrors {
		p.withErrors.Store(true)
		p.pauseReason.Store(pauseReasonTooManyErrors)
	}

	p.stopped.Store(true)
}

// newMessage returns a campaign message while internally incrementing the
// number of messages in the pipe wait group so that the status of every
// message can be atomically tracked.
func (p *pipe) newMessage(s models.Subscriber) (CampaignMessage, error) {
	msg, err := p.m.NewCampaignMessage(p.camp, s)
	if err != nil {
		return msg, err
	}

	msg.pipe = p
	p.wg.Add(1)

	return msg, nil
}

// cleanup finishes the campaign and updates the campaign status in the DB
// and also triggers a notification to the admin. This only triggers once
// a pipe's wg counter is fully exhausted, draining all messages in its queue.
func (p *pipe) cleanup() {
	defer func() {
		p.m.pipesMut.Lock()
		delete(p.m.pipes, p.camp.ID)
		p.m.pipesMut.Unlock()
	}()

	// Update campaign's 'sent count.
	if err := p.m.store.UpdateCampaignCounts(p.camp.ID, 0, int(p.sent.Load()), int(p.lastID.Load())); err != nil {
		p.m.log.Printf("error updating campaign counts (%s): %v", p.camp.Name, err)
	}

	// The campaign was auto-paused due to errors.
	if p.withErrors.Load() {
		reason, _ := p.pauseReason.Load().(string)
		if err := p.m.store.UpdateCampaignStatus(p.camp.ID, models.CampaignStatusPaused, reason); err != nil {
			p.m.log.Printf("error updating campaign (%s) status to %s: %v", p.camp.Name, models.CampaignStatusPaused, err)
		} else {
			p.m.log.Printf("set campaign (%s) to %s", p.camp.Name, models.CampaignStatusPaused)
		}

		_ = p.m.sendNotif(p.camp, models.CampaignStatusPaused, reason)
		return
	}

	// The campaign was manually stopped (pause, cancel).
	if p.stopped.Load() {
		p.m.log.Printf("stop processing campaign (%s)", p.camp.Name)
		return
	}

	// Campaign wasn't manually stopped and subscribers were naturally exhausted.
	// Fetch the up-to-date campaign status from the DB.
	c, err := p.m.store.GetCampaign(p.camp.ID)
	if err != nil {
		p.m.log.Printf("error fetching campaign (%s) for ending: %v", p.camp.Name, err)
		return
	}

	// If a running campaign has exhausted subscribers, it's finished.
	if c.Status == models.CampaignStatusRunning || c.Status == models.CampaignStatusScheduled {
		c.Status = models.CampaignStatusFinished
		if err := p.m.store.UpdateCampaignStatus(p.camp.ID, models.CampaignStatusFinished, ""); err != nil {
			p.m.log.Printf("error finishing campaign (%s): %v", p.camp.Name, err)
		} else {
			p.m.log.Printf("campaign (%s) finished", p.camp.Name)
		}
	} else {
		p.m.log.Printf("finish processing campaign (%s)", p.camp.Name)
	}

	// Notify admin.
	_ = p.m.sendNotif(c, c.Status, "")
}
