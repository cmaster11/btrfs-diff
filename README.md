# btrfs-diff

`btrfs-diff` is a 95% rewrite of the
wonderful [mbideau/btrfs-diff-go](https://github.com/mbideau/btrfs-diff-go), but fit for my own
needs :) Also does not require `libbtrfs-dev`!

Probably largely over-engineered, but it has been a fun project!

It only supports snapshot diff files generated with

```
sudo btrfs send -p PARENT_SNAPSHOT NEW_SNAPSHOT > DIFF_FILE
```

## Usage

```
# Pretty print output, for debugging
btrfs-diff DIFF_FILE

# Ignore paths matching the regexes in the output
btrfs-diff --ignore '^/var/log' --ignore '^/var/cache' DIFF_FILE 

# Output as JSON, for using the output somewhere
btrfs-diff --json DIFF_FILE
```

## Examples

Truly, I built this for myself first, so this output is very chaotic. What matters is that the paths of
added/changed/deleted files are the right ones!

```
go run . test_data/inc-010.snap
```

```
[INFO] 05:50:06.973500 === Tree ===
[INFO] 05:50:06.973505 [DIR][deleted] /bar [rel=/o258-10-0:RENAME_DEST]
[INFO] 05:50:06.973509 [UNKNOWN][deleted] /bar/baaz_file
```

```
go run . test_data/inc-010.snap --json
```

```json
{
  "added": null,
  "changed": null,
  "deleted": [
    {
      "node_type": "DIR",
      "path": "/bar",
      "state": 4,
      "relations": [
        {
          "path": "/o258-10-0",
          "reason": "RENAME_DEST"
        }
      ],
      "changes": null
    },
    {
      "node_type": "UNKNOWN",
      "path": "/bar/baaz_file",
      "state": 4,
      "relations": null,
      "changes": null
    }
  ]
}
```

## Testing

1. Edit `main_test.go` and add/alter any commands
2. Run `gen_test_data.sh` to regenerate the BTRFS snapshots
3. Fix the tests as you need