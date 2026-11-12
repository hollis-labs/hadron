package runtime

import (
	"errors"

	libsched "github.com/hollis-labs/libs/core/util/scheduler"

	"github.com/hollis-labs/substrate/module"
	"github.com/hollis-labs/substrate/modules/database"
	"github.com/hollis-labs/substrate/modules/scheduler"

	"github.com/hollis-labs/hadron/internal/appworkflow"
	"github.com/hollis-labs/hadron/internal/appworkflow/hoststate"
	"github.com/hollis-labs/hadron/internal/persistence"
)

// RegisterScheduler points the framework scheduler module at Hadron's activation
// tables, which already implement the libs scheduler Store, with the activation
// service as the runner and the activation observer and claim lease the old
// adapter used. Call it before boot.Start; pass the returned Fires to New.
func RegisterScheduler() *Fires {
	fires := &Fires{}
	scheduler.UseRunner(fires)
	scheduler.UseStoreFunc(func(app *module.App) (libsched.Store, error) {
		db := database.From(app)
		if db == nil {
			return nil, errors.New("the database module is not started")
		}
		store, err := persistence.NewWorkflowActivationStore(persistence.NewStore(db.DB()))
		if err != nil {
			return nil, err
		}
		return store, nil
	})
	scheduler.UseOptionsFunc(func(store libsched.Store) []libsched.Option {
		opts := []libsched.Option{libsched.WithClaimLease(hoststate.ActivationClaimLease)}
		if as, ok := store.(hoststate.ActivationStore); ok {
			opts = append(opts, libsched.WithObserver(appworkflow.ActivationObserver{Store: as}))
		}
		return opts
	})
	return fires
}
