package main

import (
	"regexp"
	"testing"
)

func TestFilterSnapshots(t *testing.T) {
	data := []struct{
		volumes []string
		result []string
		snapshotDir string
		r *regexp.Regexp
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
	btrfsOutput := `ID 6986 gen 23961 top level 5 path snapshot/2019-01-10_03-00
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

	expectedSubvolumes := []string{"snapshot/2019-01-10_03-00", "snapshot/2019-01-11_03-00", "snapshot/2019-01-12_03-00", "snapshot/2019-01-13_03-00", "snapshot/2019-01-14_03-00", "snapshot/2019-01-15_03-00", "snapshot/2019-01-16_03-00", "snapshot/2019-01-17_03-00", "snapshot/2019-01-18_03-00", "snapshot/2019-01-19_03-00", "snapshot/2019-01-20_03-00", "snapshot/2019-01-21_03-00", "snapshot/2019-01-22_03-00", "snapshot/2019-01-23_03-00", "snapshot/2019-01-24_03-00", "snapshot/2019-01-25_03-00", "snapshot/2019-01-26_03-00", "snapshot/2019-01-27_03-00", "snapshot/2019-01-28_03-00", "snapshot/2019-01-29_03-00", "snapshot/2019-01-30_03-00", "snapshot/2019-01-31_03-00"}

	subvolumes, err := parseSubVolumes(btrfsOutput)
	if err != nil {
		t.Fatal(err)
	}

	t.Log(subvolumes)

	if len(subvolumes) != len(expectedSubvolumes) {
		t.Fatal("length differs")
	}

	for i := range expectedSubvolumes {
		if subvolumes[i] != expectedSubvolumes[i] {
			t.Fatalf("result differs at position %d: '%s' != '%s'", i, subvolumes[i], expectedSubvolumes[i])
		}
	}
}
