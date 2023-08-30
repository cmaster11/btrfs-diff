package main

import (
	"fmt"
	"github.com/cmaster11/btrfs-diff/pkg"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strings"
	"testing"
)

type EType string

const (
	ETypeCreated  EType = "created"
	ETypeModified EType = "modified"
	ETypeDeleted  EType = "deleted"
)

type Expect struct {
	Type EType
	Path string
}
type TestDiffTest struct {
	Command string
	Expect  []*Expect
}

var testDir = path.Join("test_data")
var testCommandsFile = path.Join(testDir, "..", "test_commands")

func printExpect(diffStruct *pkg.DiffJSONStruct) {
	var ss []string

	ss = append(ss, getExpectEntries("ETypeCreated", diffStruct.Added)...)
	ss = append(ss, getExpectEntries("ETypeModified", diffStruct.Changed)...)
	ss = append(ss, getExpectEntries("ETypeDeleted", diffStruct.Deleted)...)

	fmt.Println(fmt.Sprintf(`[]*Expect{%s}`, strings.Join(ss, ", ")))
}

func getExpectEntries(et string, entries []*pkg.DiffNode) []string {
	var ss []string
	for _, entry := range entries {
		p := entry.GetChainPath()
		ss = append(ss, fmt.Sprintf(`{%s, "%s"}`, et, p))
	}
	return ss
}

func TestDiff(t *testing.T) {

	tests := []*TestDiffTest{ // []*Expect{}},
		{"echo foo > foo_file", []*Expect{{ETypeCreated, "/foo_file"}}},
		{"mkdir bar", []*Expect{{ETypeCreated, "/bar"}}},
		{"mv foo_file bar", []*Expect{{ETypeCreated, "/bar/foo_file"}, {ETypeDeleted, "/foo_file"}}},
		{"echo baz12345 > bar/baz_file", []*Expect{{ETypeCreated, "/bar/baz_file"}}},
		{"sed 's/123//' -i bar/baz_file", []*Expect{{ETypeCreated, "/bar/baz_file"}, {ETypeDeleted, "/bar/baz_file"}}},
		{"echo buzz >> bar/baz_file", []*Expect{{ETypeModified, "/bar/baz_file"}}},
		{"ln bar/baz_file bar/baaz_file", []*Expect{{ETypeCreated, "/bar/baaz_file"}}},
		{"mv bar/baz_file bar/foo_file", []*Expect{{ETypeCreated, "/bar/foo_file"}, {ETypeDeleted, "/bar/foo_file"}, {ETypeDeleted, "/bar/baz_file"}}},
		{"rm bar/foo_file", []*Expect{{ETypeDeleted, "/bar/foo_file"}}},
		{"rm -rf bar", []*Expect{{ETypeDeleted, "/bar"}, {ETypeDeleted, "/bar/baaz_file"}}},
		{"mkdir dir", []*Expect{{ETypeCreated, "/dir"}}},
		{"touch dir/file", []*Expect{{ETypeCreated, "/dir/file"}}},
		{"mkfifo dir/fifo", []*Expect{{ETypeCreated, "/dir/fifo"}}},
		{"ln dir/file dir/hardlink", []*Expect{{ETypeCreated, "/dir/hardlink"}}},
		{"ln -s file dir/symlink", []*Expect{{ETypeCreated, "/dir/symlink"}}},
		{"mv dir/hardlink dir/hardlink.rn", []*Expect{{ETypeCreated, "/dir/hardlink.rn"}, {ETypeDeleted, "/dir/hardlink"}}},
		{"mv dir/symlink dir/symlink.rn", []*Expect{{ETypeCreated, "/dir/symlink.rn"}, {ETypeDeleted, "/dir/symlink"}}},
		{"mv dir/fifo dir/fifo.rn", []*Expect{{ETypeCreated, "/dir/fifo.rn"}, {ETypeDeleted, "/dir/fifo"}}},
		{"echo todel > dir/file_to_del", []*Expect{{ETypeCreated, "/dir/file_to_del"}}},
		{"mkdir -p dir/subdir/leafdir", []*Expect{{ETypeCreated, "/dir/subdir"}, {ETypeCreated, "/dir/subdir/leafdir"}}},
		{"echo yep > dir/subdir/yep", []*Expect{{ETypeCreated, "/dir/subdir/yep"}}},
		{"echo leaf > dir/subdir/leafdir/leaf", []*Expect{{ETypeCreated, "/dir/subdir/leafdir/leaf"}}},
		{"mv dir topdir", []*Expect{{ETypeCreated, "/topdir"}, {ETypeDeleted, "/dir"}}},
		{"rm -rf topdir", []*Expect{{ETypeDeleted, "/topdir"}, {ETypeDeleted, "/topdir/hardlink.rn"}, {ETypeDeleted, "/topdir/fifo.rn"}, {ETypeDeleted, "/topdir/symlink.rn"}, {ETypeDeleted, "/topdir/file_to_del"}, {ETypeDeleted, "/topdir/file"}}},
	}

	{
		var ss []string
		// Dump all commands
		for _, test := range tests {
			ss = append(ss, test.Command)
		}
		require.NoError(t, os.WriteFile(testCommandsFile, []byte(strings.Join(ss, "\n")+"\n"), 0644))
	}

	for _, prefix := range []string{"inc", "inc-no-data"} {
		for idx, test := range tests {
			t.Run(fmt.Sprintf("%s-%s", prefix, test.Command), func(t *testing.T) {
				snapFile := fmt.Sprintf("%s/%s-%03d.snap", testDir, prefix, idx+1)
				diff, err := pkg.ProcessFile(snapFile)
				require.NoError(t, err)

				diffStr := diff.GetDiffStruct(nil)
				printExpect(diffStr)

				require.NotEmpty(t, test.Expect)

				matchedCount := 0
				for _, exp := range test.Expect {
					var testDest []*pkg.DiffNode
					switch exp.Type {
					case ETypeCreated:
						testDest = diffStr.Added
					case ETypeModified:
						testDest = diffStr.Changed
					case ETypeDeleted:
						testDest = diffStr.Deleted
					}

					found := false

					for _, entry := range testDest {
						if exp.Path == entry.GetChainPath() {
							found = true
							matchedCount++
						}
					}

					require.True(t, found)
				}

				// We make sure that all entries are tested
				require.EqualValues(t, len(diffStr.Added)+len(diffStr.Changed)+len(diffStr.Deleted), matchedCount)
			})
		}
	}

}
