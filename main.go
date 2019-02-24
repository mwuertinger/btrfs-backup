package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strings"
)

func main() {
	remote := destination{
		host: "target-host",
		port: 10022,
	}

	snapshotRegex := regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`)
	snapshotSubDir := "snapshot"

	snapshotsLocal, err := getSnapshots(localhost, "/mnt", snapshotSubDir, snapshotRegex)
	if err != nil {
		log.Fatal(err)
	}
	snapshotsRemote, err := getSnapshots(remote, "/mnt", "", snapshotRegex)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("local:")
	for _, snapshot := range snapshotsLocal {
		fmt.Println(snapshot)
	}
	fmt.Println("\nremote:")
	for _, snapshots := range snapshotsRemote {
		fmt.Println(snapshots)
	}

	mostRecentRemote := snapshotsRemote[len(snapshotsRemote)-1]
	previousSnapshot := ""
	for _, snapshot := range snapshotsLocal {
		if previousSnapshot != "" {
			err := sendSnapshot(snapshot, previousSnapshot, "/mnt", snapshotSubDir, remote)
			if err != nil {
				log.Fatal(err)
			}
			previousSnapshot = snapshot
		}

		if snapshot == mostRecentRemote {
			previousSnapshot = mostRecentRemote
		}
	}
}

func sendSnapshot(snapshot, previousSnapshot, mountPoint, prefix string, d destination) error {
	fmt.Printf("btrfs send -p %s %s\n", path.Join(mountPoint, prefix, previousSnapshot), path.Join(mountPoint, prefix, snapshot))
	return nil
}

func getSnapshots(d destination, mountPoint string, snapshotDir string, r *regexp.Regexp) ([]string, error) {
	subVolumes, err := execPipe(d, parseSubVolumes, "btrfs", "subvolume", "list", mountPoint)
	if err != nil {
		return nil, err
	}
	snapshots := filterSnapshots(subVolumes, snapshotDir, r)
	sort.Strings(snapshots)
	return snapshots, nil
}

func filterSnapshots(volumes []string, snapshotDir string, r *regexp.Regexp) []string {
	var snapshots []string
	for _, volume := range volumes {
		dir, name := path.Split(volume)
		if strings.TrimSuffix(dir, "/") != snapshotDir {
			log.Printf("dir != snapshotDir: %s != %s", dir, snapshotDir)
			continue
		}
		if !r.MatchString(name) {
			log.Printf("%s does not match %v", name, *r)
			continue
		}
		snapshots = append(snapshots, name)
	}
	return snapshots
}

// parser parses the output of a process and returns the result as a slice of strings.
type parser func(r io.Reader) ([]string, error)

// destination determines where a command is to be executed.
type destination struct {
	host string
	port int
}

// localhost is a special destination for running commands locally
var localhost destination

// execPipe runs the specified cmd at destination d and parses the result with p before returning it.
func execPipe(d destination, p parser, cmd ... string) ([]string, error) {
	var c *exec.Cmd
	if d == localhost {
		c = exec.Command(cmd[0], cmd[1:]...)
	} else {
		c = exec.Command("ssh", fmt.Sprintf("-p%d", d.port), d.host, strings.Join(cmd, " "))
	}

	pipe, err := c.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("execPipe: %v", err)
	}
	defer pipe.Close()
	if err := c.Start(); err != nil {
		return nil, fmt.Errorf("execPipe: %v", err)
	}

	res, err := p(pipe)
	if err != nil {
		return nil, fmt.Errorf("execPipe: %v", err)
	}

	if err := c.Wait(); err != nil {
		return nil, fmt.Errorf("execPipe: %v", err)
	}

	return res, nil
}

// parseSubVolumes extracts the sub-volume names from the "btrfs subvolume list" command.
func parseSubVolumes(r io.Reader) ([]string, error) {
	br := bufio.NewReader(r)
	var names []string
	for {
		line, err := br.ReadBytes('\n')
		if err == io.EOF {
			return names, nil
		}
		tokens := strings.Split(string(line), " ")
		if len(tokens) != 9 {
			return nil, fmt.Errorf("parseSubVolumes: unexpected btrfs output: %s", line)
		}
		names = append(names, strings.TrimRight(tokens[8], "\n"))
	}


	return names, nil
}
