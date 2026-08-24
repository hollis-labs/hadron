// Package gate defines the extraction-ready vocabulary shared by durable
// human gates and checkpoints.
//
// Gate declarations describe presentation and policy subjects; they never
// embed product approver lists or authenticate responders. Applications own
// presentation, payload persistence, and authority resolution through the
// interfaces in this package. Durable suspension and resume remain owned by
// workflow/wait and the runtime wait coordinator.
//
// This package may import only extraction-safe workflow contracts and the Go
// standard library. Its exported vocabulary is stable; concrete application
// policy and UI models must remain outside workflow/.
package gate
