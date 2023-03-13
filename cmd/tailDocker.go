package cmd

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/tomekkolo/immudb-play/pkg/service"
	"github.com/tomekkolo/immudb-play/pkg/source"
)

var tailDockerCmd = &cobra.Command{
	Use:   "docker <collection> <container>",
	Short: "Tail from docker logs and store audit data in immudb",
	RunE:  tailDocker,
	Args:  cobra.ExactArgs(2),
}

func tailDocker(cmd *cobra.Command, args []string) error {
	err := runParentCmdE(cmd, args)
	if err != nil {
		return err
	}

	log.WithField("args", args).Info("Docker tail")

	err = configure(args[0])
	if err != nil {
		return err
	}

	flagSince, _ := cmd.Flags().GetString("since")
	flagStdout, _ := cmd.Flags().GetBool("stdout")
	flagStderr, _ := cmd.Flags().GetBool("stderr")
	dockerTail, err := source.NewDockerTail(args[1], flagFollow, flagSince, flagStdout, flagStderr)
	if err != nil {
		return fmt.Errorf("invalide source: %w", err)
	}

	s := service.NewAuditService(dockerTail, lp, jsonRepository)
	return s.Run()
}

func init() {
	tailCmd.AddCommand(tailDockerCmd)
	tailDockerCmd.Flags().String("since", "", "since argument")
	tailDockerCmd.Flags().Bool("stdout", false, "If true, read stdout from container")
	tailDockerCmd.Flags().Bool("stderr", false, "If true, read stderr from container")
}
