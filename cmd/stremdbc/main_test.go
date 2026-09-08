package main

import (
	"sync"
	"testing"
)

func TestNewShutdownHandlesNilDependencies(t *testing.T) {
	shutdown := newShutdown(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	if err := shutdown(nil); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}
}

func TestNewShutdownIsSafeWhenCalledConcurrently(t *testing.T) {
	shutdown := newShutdown(nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := shutdown(nil); err != nil {
				t.Errorf("shutdown returned error: %v", err)
			}
		}()
	}
	wg.Wait()
}
