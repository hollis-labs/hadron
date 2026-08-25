package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	address := flag.String("addr", defaultDaemonAddress, "hadrond listen address")
	noBrowser := flag.Bool("no-browser", false, "print the operator UI URL without opening it")
	flag.Parse()

	app, err := newDesktopApp(*address)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := app.run(ctx, !*noBrowser); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
