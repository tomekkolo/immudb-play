package source

import (
	"bufio"
	"context"
	"fmt"
	"io"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
)

type DockerTail struct {
	reader  io.ReadCloser
	scanner *bufio.Scanner
}

func NewDockerTail() (*DockerTail, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("could not create docker client, %w", err)
	}

	reader, err := cli.ContainerLogs(context.TODO(), "some", types.ContainerLogsOptions{Follow: true, Since: "0m"})
	if err != nil {
		return nil, fmt.Errorf("could not create docker logs reader")
	}
	scanner := bufio.NewScanner(reader)

	return &DockerTail{
		reader:  reader,
		scanner: scanner,
	}, nil
}

func (dt *DockerTail) ReadLine() (string, error) {
	dt.scanner.Scan()
	return dt.scanner.Text(), nil
}
