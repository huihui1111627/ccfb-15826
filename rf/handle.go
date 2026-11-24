package rf

import (
	"sync"
	"time"
)

// Handle is the probe-side endpoint of a runtime filter. The hash join
// publishes the merged filter into it once the build side finishes; scans
// wait on it with a bounded timeout, then proceed unfiltered if it is late.
type Handle struct {
	cfg Config
	mu  sync.Mutex
	rf  *RuntimeFilter
	ch  chan struct{}

	// published is closed once the filter is delivered.
	published bool
	closed    bool
}

// NewHandle creates an empty probe-side handle.
func NewHandle(cfg Config) *Handle {
	return &Handle{cfg: cfg, ch: make(chan struct{})}
}

// Publish delivers the merged filter. Safe to call once.
func (h *Handle) Publish(f *RuntimeFilter) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.rf = f
	h.closed = true
	close(h.ch)
}

// PublishEmpty delivers a nil filter (empty build side of an inner join:
// every probe row must be rejected; we model that with a published
// minmax/empty marker handled by the executor, so simply deliver nil and let
// callers interpret).
func (h *Handle) PublishEmpty() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	close(h.ch)
}

// Wait blocks up to MaxWaitMS for the filter. Returns (nil, false) on
// timeout: the scan should start unfiltered and consult TryGet later.
func (h *Handle) Wait() (*RuntimeFilter, bool) {
	timeout := time.Duration(h.cfg.MaxWaitMS) * time.Millisecond
	if timeout <= 0 {
		select {
		case <-h.ch:
			h.mu.Lock()
			f := h.rf
			h.mu.Unlock()
			return f, true
		default:
			return nil, false
		}
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case <-h.ch:
		h.mu.Lock()
		f := h.rf
		h.mu.Unlock()
		return f, true
	case <-t.C:
		return nil, false
	}
}

// TryGet returns the filter if it has arrived, plus the readiness flag.
func (h *Handle) TryGet() (*RuntimeFilter, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.rf, h.closed
}

// IsReady reports whether a filter has been published.
func (h *Handle) IsReady() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.closed
}
