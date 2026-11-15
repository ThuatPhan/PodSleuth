// Command podsleuth is the PodSleuth CLI entrypoint and composition root.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ThuatPhan/PodSleuth/internal/cli"
)

func main() {
	os.Exit(run())
}

func run() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	err := cli.Execute(ctx, cli.Deps{}, os.Args[1:])
	if err == nil {
		return 0
	}
	if errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "podsleuth: interrupted")
		return 1
	}
	fmt.Fprintln(os.Stderr, err)
	return exitCode(err)
}

func exitCode(err error) int {
	var ec cli.ExitCoder
	if errors.As(err, &ec) {
		return ec.ExitCode()
	}
	return 1
}
