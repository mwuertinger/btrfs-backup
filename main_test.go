package main

import (
	"fmt"
	"reflect"
	"regexp"
	"testing"
)

func TestParseNode(t *testing.T) {
	data := []struct {
		in  string
		out node
		err bool
	}{
		{
			in: "foo.bar:123/fizz/buzz",
			out: node{
				address:    "foo.bar",
				sshPort:    123,
				mountPoint: "/fizz/buzz",
			},
			err: false,
		},
	}

	for _, d := range data {
		out, err := parseNode(d.in)
		if d.err && err == nil {
			t.Errorf("expected error but succeeded")
		}
		if !reflect.DeepEqual(out, d.out) {
			t.Errorf("unexpected output: %v", out)
		}
	}
}

// mockExecutor returns (res, err) if exec is invoked with cmd and returns an error otherwise.
type mockExecutor struct {
	cmds [][]string
	res  string
	err  error
}

func (e mockExecutor) exec(cmds [][]string) (string, int, error) {
	if !reflect.DeepEqual(cmds, e.cmds) {
		return "", 0, fmt.Errorf("unexpected cmd: %#v", cmds)
	}
	return e.res, 0, e.err
}

func TestGetSnapshots(t *testing.T) {
	data := []struct {
		node      node
		snapshots []string
		err       bool
	}{
		{
			node: node{
				executor: mockExecutor{
					[][]string{{"btrfs", "subvolume", "list", "/foo"}},
					"ID 6988 gen 23968 top level 5 path snapshot/2019-01-11_03-00\nID 6989 gen 23981 top level 5 path snapshot/2019-01-12_03-00\nID 6990 gen 24002 top level 5 path snapshot/2019-01-13_03-00\n",
					nil,
				},
				mountPoint:    "/foo",
				snapshotPath:  "snapshot",
				snapshotRegex: regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
			},
			snapshots: []string{"2019-01-11_03-00", "2019-01-12_03-00", "2019-01-13_03-00"},
			err:       false,
		},
		{
			node: node{
				executor: mockExecutor{
					[][]string{{"btrfs", "subvolume", "list", "/foo"}},
					"",
					fmt.Errorf("mock error"),
				},
				mountPoint:    "/foo",
				snapshotPath:  "snapshot",
				snapshotRegex: regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
			},
			snapshots: []string{},
			err:       true,
		},
		{
			node: node{
				executor: mockExecutor{
					[][]string{{"btrfs", "subvolume", "list", "/foo"}},
					"foo",
					nil,
				},
				mountPoint:    "/foo",
				snapshotPath:  "snapshot",
				snapshotRegex: regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
			},
			snapshots: []string{},
			err:       true,
		},
	}

	for di, d := range data {
		res, err := d.node.getSnapshots()
		if d.err && err == nil {
			t.Errorf("%d: expected error but succeeded", di)
			continue
		}
		if !d.err && err != nil {
			t.Errorf("%d: unexpected error: %v", di, err)
			continue
		}
		if len(res) != len(d.snapshots) {
			t.Errorf("%d: unexpected number of results: %d != %d", di, len(res), len(d.snapshots))
			continue
		}
		for i := range res {
			if res[i] != d.snapshots[i] {
				t.Errorf("%d: unexpected result: %#v", di, res)
				continue
			}
		}
	}
}

func TestFilterSnapshots(t *testing.T) {
	data := []struct {
		volumes     []string
		result      []string
		snapshotDir string
		r           *regexp.Regexp
	}{
		{
			[]string{"snapshot/2019-01-10_03-00", "snapshot/2019-01-11_03-00", "snapshot/2019-01-12_03-00", "snapshot/foobar", "foobar"},
			[]string{"2019-01-10_03-00", "2019-01-11_03-00", "2019-01-12_03-00"},
			"snapshot",
			regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
		},
		{
			[]string{},
			[]string{},
			"snapshot",
			regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
		},
		{
			[]string{"snapshot/2019-01-10_03-00", "foobar"},
			[]string{"2019-01-10_03-00"},
			"snapshot/",
			regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
		},
		{
			[]string{"2019-01-10_03-00", "foobar"},
			[]string{"2019-01-10_03-00"},
			"",
			regexp.MustCompile(`^\d\d\d\d-\d\d-\d\d_\d\d-\d\d$`),
		},
		{
			[]string{"2019-01-10_03-00", "foo", "bar"},
			[]string{"2019-01-10_03-00", "foo", "bar"},
			"",
			regexp.MustCompile(`.*`),
		},
	}

	for di, d := range data {
		res := filterSnapshots(d.volumes, d.snapshotDir, d.r)
		if len(res) != len(d.result) {
			t.Errorf("%d: unexpected number of results: %d != %d", di, len(res), len(d.result))
			continue
		}
		for i := range d.result {
			if res[i] != d.result[i] {
				t.Errorf("%d: unexpected result: %#v != %#v", di, res, d.result)
			}
		}
	}
}

func TestParseSubvolumes(t *testing.T) {
	longBtrfsOutput := `ID 6986 gen 23961 top level 5 path snapshot/2019-01-10_03-00
ID 6988 gen 23968 top level 5 path snapshot/2019-01-11_03-00
ID 6989 gen 23981 top level 5 path snapshot/2019-01-12_03-00
ID 6990 gen 24002 top level 5 path snapshot/2019-01-13_03-00
ID 6992 gen 24055 top level 5 path snapshot/2019-01-14_03-00
ID 6993 gen 24105 top level 5 path snapshot/2019-01-15_03-00
ID 7562 gen 24525 top level 5 path snapshot/2019-01-16_03-00
ID 7564 gen 24529 top level 5 path snapshot/2019-01-17_03-00
ID 7565 gen 24695 top level 5 path snapshot/2019-01-18_03-00
ID 7566 gen 24776 top level 5 path snapshot/2019-01-19_03-00
ID 7567 gen 24791 top level 5 path snapshot/2019-01-20_03-00
ID 7568 gen 24803 top level 5 path snapshot/2019-01-21_03-00
ID 7569 gen 24809 top level 5 path snapshot/2019-01-22_03-00
ID 7570 gen 24823 top level 5 path snapshot/2019-01-23_03-00
ID 7571 gen 24828 top level 5 path snapshot/2019-01-24_03-00
ID 7572 gen 24829 top level 5 path snapshot/2019-01-25_03-00
ID 7573 gen 24830 top level 5 path snapshot/2019-01-26_03-00
ID 7574 gen 24831 top level 5 path snapshot/2019-01-27_03-00
ID 7575 gen 24899 top level 5 path snapshot/2019-01-28_03-00
ID 7576 gen 24932 top level 5 path snapshot/2019-01-29_03-00
ID 7577 gen 24965 top level 5 path snapshot/2019-01-30_03-00
ID 7578 gen 24969 top level 5 path snapshot/2019-01-31_03-00
`

	data := []struct {
		btrfsOutput string
		res         []string
		err         bool
	}{
		{
			longBtrfsOutput,
			[]string{"snapshot/2019-01-10_03-00", "snapshot/2019-01-11_03-00", "snapshot/2019-01-12_03-00", "snapshot/2019-01-13_03-00", "snapshot/2019-01-14_03-00", "snapshot/2019-01-15_03-00", "snapshot/2019-01-16_03-00", "snapshot/2019-01-17_03-00", "snapshot/2019-01-18_03-00", "snapshot/2019-01-19_03-00", "snapshot/2019-01-20_03-00", "snapshot/2019-01-21_03-00", "snapshot/2019-01-22_03-00", "snapshot/2019-01-23_03-00", "snapshot/2019-01-24_03-00", "snapshot/2019-01-25_03-00", "snapshot/2019-01-26_03-00", "snapshot/2019-01-27_03-00", "snapshot/2019-01-28_03-00", "snapshot/2019-01-29_03-00", "snapshot/2019-01-30_03-00", "snapshot/2019-01-31_03-00"},
			false,
		},
		{
			"ID 7564 gen 24529 top level 5 path foo",
			[]string{"foo"},
			false,
		},
		{
			"\n\nID 7564 gen 24529 top level 5 path foo\n\nID 7564 gen 24529 top level 5 path bar\n",
			[]string{"foo", "bar"},
			false,
		},
		{
			"foo bar fizz buzz foo bar fizz buzz foo",
			[]string{"foo"},
			false,
		},
		{
			"foo bar fizz buzz foo bar fizz buzz foo bar",
			[]string{},
			true,
		},
	}

	for di, d := range data {
		res, err := parseSubVolumes(d.btrfsOutput)
		if d.err && err == nil {
			t.Errorf("%d: expected error but succedded", di)
			continue
		}
		if !d.err && err != nil {
			t.Errorf("%d: unexpected error: %v", di, err)
			continue
		}
		if len(res) != len(d.res) {
			t.Errorf("%d: length differs", di)
			continue
		}
		for i := range d.res {
			if res[i] != d.res[i] {
				t.Fatalf("%d: result differs: %#v != %#v", di, res, d.res)
			}
		}
	}

}

func TestExec(t *testing.T) {
	data := []struct {
		cmds [][]string
		err  bool
		res  string
	}{
		{
			[][]string{{"/bin/true"}},
			false,
			"",
		},
		{
			[][]string{{"/bin/false"}},
			true,
			"",
		},
		{
			[][]string{{"/foo/bar/fizz/buzz"}},
			true,
			"",
		},
		{
			[][]string{{"echo", "foo"}},
			false,
			"foo\n",
		},
	}

	for di, d := range data {
		res, _, err := defaultExecutor.exec(d.cmds)
		if d.err && err == nil {
			t.Errorf("%d: expected error but succeeded", di)
			continue
		}
		if !d.err && err != nil {
			t.Errorf("%d: unexpected error: %v", di, err)
			continue
		}
		if res != d.res {
			t.Errorf("%d: unexpected output: %s", di, res)
		}
	}
}

func TestExecPipe(t *testing.T) {
	out, _, err := defaultExecutor.exec([][]string{{"echo", "foo"}, {"cat"}})
	if err != nil {
		t.Error(err)
	}
	if out != "foo\n" {
		t.Errorf("unexpected output: %s", out)
	}
}

type trackingExecutor struct {
	invocations []invocation
}

type invocation struct {
	cmds [][]string
}

func (e *trackingExecutor) exec(cmds [][]string) (string, int, error) {
	e.invocations = append(e.invocations, invocation{cmds})
	return "", 0, nil
}

func TestTransmitSnapshots(t *testing.T) {
	data := []struct {
		localSnapshots  []string
		remoteSnapshots []string
		source          node
		destination     node
		invocations     []invocation
	}{
		{
			localSnapshots:  []string{"1", "2", "3", "4", "5"},
			remoteSnapshots: []string{"1", "2", "3"},
			source: node{
				mountPoint:   "/foo",
				snapshotPath: "bar",
			},
			destination: node{
				address:    "foo",
				sshPort:    123,
				mountPoint: "/foo",
			},
			invocations: []invocation{
				{[][]string{{"btrfs", "send", "--quiet", "-p", "/foo/bar/3", "/foo/bar/4"}, {"ssh", "-C", "-p123", "foo", "--", "btrfs", "receive", "/foo"}}},
				{[][]string{{"btrfs", "send", "--quiet", "-p", "/foo/bar/4", "/foo/bar/5"}, {"ssh", "-C", "-p123", "foo", "--", "btrfs", "receive", "/foo"}}},
			},
		},
	}

	for di, d := range data {
		exec := &trackingExecutor{}
		d.source.executor = exec
		err := transmitSnapshots(&d.source, &d.destination, d.localSnapshots, d.remoteSnapshots, false)
		if err != nil {
			t.Errorf("%d: unexpected error: %v", di, err)
			continue
		}

		if !reflect.DeepEqual(exec.invocations, d.invocations) {
			t.Errorf("%d: unexpected invocations: %#v", di, exec.invocations)
		}
	}
}

func TestFormatBytes(t *testing.T) {
	data := []struct {
		in  int
		out string
	}{
		{0, "0.0 B"},
		{10, "10.0 B"},
		{1023, "1023.0 B"},
		{1024, "1.0 kiB"},
		{1025, "1.0 kiB"},
		{1024 * 1024, "1.0 MiB"},
		{1024 * 1024 * 1024 * 1024 * 1024, "1024.0 TiB"},
	}

	for _, d := range data {
		t.Run(fmt.Sprintf("%v", d), func(t *testing.T) {
			out := formatBytes(d.in)
			if out != d.out {
				t.Errorf("%s != %s", out, d.out)
			}
		})
	}
}
