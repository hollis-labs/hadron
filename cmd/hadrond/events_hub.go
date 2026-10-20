package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hollis-labs/hadron/internal/hubrelay"
	"github.com/hollis-labs/hadron/internal/persistence"
	"github.com/hollis-labs/libs/mesh/eventbus"
	"github.com/hollis-labs/libs/mesh/eventbus/relay"
)

// hubRelayStarter returns what the runtime calls at Start to ship the event
// outbox to the event hub, or nil when the relay is off. The hub is found
// through the environment (HOLLIS_EVENTS_URL and the app's token file, as in
// mesh/eventbus); with no URL there is no hub and nothing runs. It logs and
// carries on when it cannot start: shipping events is never a reason to take
// the daemon down.
//
// The relay reads the outbox, which is durable, and moves its cursor (a file
// under <data dir>/events-hub, one per store generation) only over events the
// hub settled or the client durably quarantined. A hub that is down, or a
// missing or under-scoped credential, holds events back and loses none.
func hubRelayStarter(state *persistence.WorkflowStateStore, dataDir string) func(context.Context) (stop func()) {
	return func(ctx context.Context) func() {
		generation, err := state.StoreGeneration(ctx)
		if err != nil {
			log.Printf("[hub] store generation: %v", err)
			return func() {}
		}
		dir := filepath.Join(dataDir, "events-hub")
		if mkErr := os.MkdirAll(filepath.Join(dir, "spool"), 0o700); mkErr != nil {
			log.Printf("[hub] state dir: %v", mkErr)
			return func() {}
		}
		pub, err := eventbus.FromEnv("hadron", eventbus.Config{
			SpoolDir: filepath.Join(dir, "spool"),
			OnError:  func(err error) { log.Printf("[hub] client: %v", err) },
		})
		if err != nil {
			log.Printf("[hub] client: %v", err)
			return func() {}
		}
		deliverer, ok := pub.(relay.Deliverer)
		if !ok {
			log.Printf("[hub] relay is on but %s is not set: nothing to send to", eventbus.EnvURL)
			return func() {}
		}
		r, err := relay.New(relay.Config{
			Source:    hubrelay.Source{Store: state},
			Cursor:    relay.FileCursor{Path: filepath.Join(dir, "outbox-"+generation+".cursor")},
			Deliverer: deliverer,
			OnError:   func(err error) { log.Printf("[hub] relay: %v", err) },
		})
		if err != nil {
			log.Printf("[hub] relay: %v", err)
			return func() {}
		}
		log.Printf("[hub] relaying the event outbox (store generation %s) to the hub", generation)
		runCtx, cancel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			if runErr := r.Run(runCtx); runErr != nil && runCtx.Err() == nil {
				log.Printf("[hub] relay stopped: %v", runErr)
			}
			// runCtx is already done at shutdown; the last delivery attempt needs a
			// context that is not canceled with it.
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(runCtx), 5*time.Second)
			defer closeCancel()
			if closeErr := pub.Close(closeCtx); closeErr != nil {
				log.Printf("[hub] close: %v", closeErr)
			}
		}()
		return func() {
			cancel()
			wg.Wait()
		}
	}
}
