package retryutil

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

type mockTicker struct {
	test    *testing.T
	counter int
}

func (t *mockTicker) Stop() {}

func (t *mockTicker) Tick() {
	t.counter++
}

func TestRetryWorkerSuccess(t *testing.T) {
	tick := &mockTicker{t, 0}
	result := RetryWorker(10, 20, tick, func() (bool, error) {
		return true, nil
	})

	if result != nil {
		t.Errorf("Wrong result, expected: %#v, got: %#v", nil, result)
	}

	if tick.counter != 0 {
		t.Errorf("Ticker was started once, but it shouldn't be")
	}
}

func TestRetryWorkerOneFalse(t *testing.T) {
	var counter = 0

	tick := &mockTicker{t, 0}
	result := RetryWorker(1, 3, tick, func() (bool, error) {
		counter++
		return counter > 1, nil
	})

	if result != nil {
		t.Errorf("Wrong result, expected: %#v, got: %#v", nil, result)
	}

	if tick.counter != 1 {
		t.Errorf("Ticker was started %#v, but supposed to be just once", tick.counter)
	}
}

func TestRetryWorkerError(t *testing.T) {
	fail := errors.New("Error")

	tick := &mockTicker{t, 0}
	result := RetryWorker(1, 3, tick, func() (bool, error) {
		return false, fail
	})

	if result != fail {
		t.Errorf("Wrong result, expected: %#v, got: %#v", fail, result)
	}
}

func TestRetryWithBackoffWorker(t *testing.T) {
	fail := errors.New("Error")

	tests := []struct {
		name          string
		succeedAfter  int
		err           error
		expectedSleep []time.Duration
		expectErr     bool
	}{
		{"succeeds at once", 1, nil, nil, false},
		{"interval doubles up to the max", 5, nil, []time.Duration{1, 2, 4, 5}, false},
		{"gives up at the timeout", 100, nil, []time.Duration{1, 2, 4, 5, 5}, true},
		{"stops on error", 100, fail, nil, true},
	}

	for _, tt := range tests {
		var calls int
		var slept []time.Duration
		result := RetryWithBackoffWorker(1, 5, 20, func(d time.Duration) { slept = append(slept, d) }, func() (bool, error) {
			calls++
			if tt.err != nil {
				return false, tt.err
			}
			return calls >= tt.succeedAfter, nil
		})

		if (result != nil) != tt.expectErr {
			t.Errorf("%s: unexpected result %v", tt.name, result)
		}
		if !reflect.DeepEqual(slept, tt.expectedSleep) {
			t.Errorf("%s: expected sleeps %v, got %v", tt.name, tt.expectedSleep, slept)
		}
	}
}
