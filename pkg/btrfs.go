package pkg

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

// operation is the result of one or more instructions/commands
type operation int

// list of available operations
const (
	opUnspec operation = iota
	opIgnore
	opCreate
	opModify
	opDelete
	opRename
	opEnd
)

// operation's names
var names []string = []string{"noop", "ignored", "added", "changed", "deleted", "renamed", "END"}

// convert an operation to a string
func (op operation) String() string {
	return names[op]
}

// commandMapOp is the mapping between a command and a resulting operation
type commandMapOp struct {
	Name string
	Op   operation
}

// commandInst is the instanciation of a command and its data
type commandInst struct {
	OriginalType uint16
	Type         *commandMapOp
	data         []byte
}

// initCommandsDefinitions initialize the commands mapping with operations
func initCommandsDefinitions() *[BTRFS_SEND_C_MAX_PLUS_ONE]commandMapOp {

	var commandsDefs [BTRFS_SEND_C_MAX_PLUS_ONE]commandMapOp
	commandsDefs[BTRFS_SEND_C_UNSPEC] = commandMapOp{Name: "BTRFS_SEND_C_UNSPEC", Op: opUnspec}

	commandsDefs[BTRFS_SEND_C_SUBVOL] = commandMapOp{Name: "BTRFS_SEND_C_SUBVOL", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_SNAPSHOT] = commandMapOp{Name: "BTRFS_SEND_C_SNAPSHOT", Op: opCreate}

	commandsDefs[BTRFS_SEND_C_MKFILE] = commandMapOp{Name: "BTRFS_SEND_C_MKFILE", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_MKDIR] = commandMapOp{Name: "BTRFS_SEND_C_MKDIR", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_MKNOD] = commandMapOp{Name: "BTRFS_SEND_C_MKNOD", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_MKFIFO] = commandMapOp{Name: "BTRFS_SEND_C_MKFIFO", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_MKSOCK] = commandMapOp{Name: "BTRFS_SEND_C_MKSOCK", Op: opCreate}
	commandsDefs[BTRFS_SEND_C_SYMLINK] = commandMapOp{Name: "BTRFS_SEND_C_SYMLINK", Op: opCreate}

	commandsDefs[BTRFS_SEND_C_LINK] = commandMapOp{Name: "BTRFS_SEND_C_LINK", Op: opRename}
	commandsDefs[BTRFS_SEND_C_RENAME] = commandMapOp{Name: "BTRFS_SEND_C_RENAME", Op: opRename}

	commandsDefs[BTRFS_SEND_C_UNLINK] = commandMapOp{Name: "BTRFS_SEND_C_UNLINK", Op: opDelete}
	commandsDefs[BTRFS_SEND_C_RMDIR] = commandMapOp{Name: "BTRFS_SEND_C_RMDIR", Op: opDelete}

	commandsDefs[BTRFS_SEND_C_WRITE] = commandMapOp{Name: "BTRFS_SEND_C_WRITE", Op: opModify}
	commandsDefs[BTRFS_SEND_C_CLONE] = commandMapOp{Name: "BTRFS_SEND_C_CLONE", Op: opModify}
	commandsDefs[BTRFS_SEND_C_TRUNCATE] = commandMapOp{Name: "BTRFS_SEND_C_TRUNCATE", Op: opModify}

	commandsDefs[BTRFS_SEND_C_CHMOD] = commandMapOp{Name: "BTRFS_SEND_C_CHMOD", Op: opModify}
	commandsDefs[BTRFS_SEND_C_CHOWN] = commandMapOp{Name: "BTRFS_SEND_C_CHOWN", Op: opModify}
	commandsDefs[BTRFS_SEND_C_UTIMES] = commandMapOp{Name: "BTRFS_SEND_C_UTIMES", Op: opIgnore}
	commandsDefs[BTRFS_SEND_C_SET_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_SET_XATTR", Op: opModify}
	commandsDefs[BTRFS_SEND_C_REMOVE_XATTR] = commandMapOp{Name: "BTRFS_SEND_C_REMOVE_XATTR", Op: opModify}

	commandsDefs[BTRFS_SEND_C_END] = commandMapOp{Name: "BTRFS_SEND_C_END", Op: opEnd}
	commandsDefs[BTRFS_SEND_C_UPDATE_EXTENT] = commandMapOp{Name: "BTRFS_SEND_C_UPDATE_EXTENT", Op: opModify}

	// --- Unsupported V2/3

	/* Version 2 */
	commandsDefs[BTRFS_SEND_C_FALLOCATE] = commandMapOp{Name: "BTRFS_SEND_C_FALLOCATE", Op: opIgnore}
	commandsDefs[BTRFS_SEND_C_FILEATTR] = commandMapOp{Name: "BTRFS_SEND_C_FILEATTR", Op: opIgnore}
	commandsDefs[BTRFS_SEND_C_ENCODED_WRITE] = commandMapOp{Name: "BTRFS_SEND_C_ENCODED_WRITE", Op: opIgnore}

	/* Version 3 */
	commandsDefs[BTRFS_SEND_C_ENABLE_VERITY] = commandMapOp{Name: "BTRFS_SEND_C_ENABLE_VERITY", Op: opIgnore}

	// Sanity check (hopefully no holes).
	for i, command := range commandsDefs {
		if i != BTRFS_SEND_C_UNSPEC && command.Op == opUnspec {
			return nil
		}
	}
	return &commandsDefs
}

type attrMapping struct {
	Name      string
	converter func(b []byte) interface{}
}

type bytesData struct {
	bytes  []byte
	isUTF8 bool
}

func (d bytesData) String() string {
	if d.isUTF8 {
		return ellipsis(string(d.bytes), 32)
	}
	return fmt.Sprintf("bytes:len=%d", len(d.bytes))
}

func attrConverterBytes(b []byte) interface{} {
	// Try to find out the data type
	return &bytesData{
		bytes:  b,
		isUTF8: utf8.Valid(b),
	}
}
func attrConverterUint64(b []byte) interface{} {
	return binary.LittleEndian.Uint64(b)
}
func attrConverterString(b []byte) interface{} {
	return string(b)
}
func attrConverterPath(b []byte) interface{} {
	path := string(b)
	// Remove any duplicate begin slashes
	path = strings.TrimLeft(path, "/")
	return path
}
func attrConverterPathLink(b []byte) interface{} {
	path := string(b)
	// Do not touch paths
	return path
}
func attrConverterUUID(b []byte) interface{} {
	return hex.EncodeToString(b)
}
func attrConverterTime(b []byte) interface{} {
	sec := binary.LittleEndian.Uint64(b[:8])
	nsec := binary.LittleEndian.Uint32(b[8:12])
	return time.Unix(int64(sec), int64(nsec))
}

// initAttributeDefinitions initialize the attribute mapping with their debugging names
func initAttributeDefinitions() *[BTRFS_SEND_A_MAX_V1_PLUS_ONE]attrMapping {
	var attrDefs [BTRFS_SEND_A_MAX_V1_PLUS_ONE]attrMapping

	attrDefs[BTRFS_SEND_A_UNSPEC] = attrMapping{"BTRFS_SEND_A_UNSPEC", nil}
	attrDefs[BTRFS_SEND_A_UUID] = attrMapping{"BTRFS_SEND_A_UUID", attrConverterUUID}
	attrDefs[BTRFS_SEND_A_CTRANSID] = attrMapping{"BTRFS_SEND_A_CTRANSID", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_INO] = attrMapping{"BTRFS_SEND_A_INO", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_SIZE] = attrMapping{"BTRFS_SEND_A_SIZE", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_MODE] = attrMapping{"BTRFS_SEND_A_MODE", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_UID] = attrMapping{"BTRFS_SEND_A_UID", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_GID] = attrMapping{"BTRFS_SEND_A_GID", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_RDEV] = attrMapping{"BTRFS_SEND_A_RDEV", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_CTIME] = attrMapping{"BTRFS_SEND_A_CTIME", attrConverterTime}
	attrDefs[BTRFS_SEND_A_MTIME] = attrMapping{"BTRFS_SEND_A_MTIME", attrConverterTime}
	attrDefs[BTRFS_SEND_A_ATIME] = attrMapping{"BTRFS_SEND_A_ATIME", attrConverterTime}
	attrDefs[BTRFS_SEND_A_OTIME] = attrMapping{"BTRFS_SEND_A_OTIME", attrConverterTime}
	attrDefs[BTRFS_SEND_A_XATTR_NAME] = attrMapping{"BTRFS_SEND_A_XATTR_NAME", attrConverterString}
	attrDefs[BTRFS_SEND_A_XATTR_DATA] = attrMapping{"BTRFS_SEND_A_XATTR_DATA", attrConverterBytes}
	attrDefs[BTRFS_SEND_A_PATH] = attrMapping{"BTRFS_SEND_A_PATH", attrConverterPath}
	attrDefs[BTRFS_SEND_A_PATH_TO] = attrMapping{"BTRFS_SEND_A_PATH_TO", attrConverterPath}
	attrDefs[BTRFS_SEND_A_PATH_LINK] = attrMapping{"BTRFS_SEND_A_PATH_LINK", attrConverterPathLink}
	attrDefs[BTRFS_SEND_A_FILE_OFFSET] = attrMapping{"BTRFS_SEND_A_FILE_OFFSET", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_DATA] = attrMapping{"BTRFS_SEND_A_DATA", attrConverterBytes}
	attrDefs[BTRFS_SEND_A_CLONE_UUID] = attrMapping{"BTRFS_SEND_A_CLONE_UUID", attrConverterUUID}
	attrDefs[BTRFS_SEND_A_CLONE_CTRANSID] = attrMapping{"BTRFS_SEND_A_CLONE_CTRANSID", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_CLONE_PATH] = attrMapping{"BTRFS_SEND_A_CLONE_PATH", attrConverterPath}
	attrDefs[BTRFS_SEND_A_CLONE_OFFSET] = attrMapping{"BTRFS_SEND_A_CLONE_OFFSET", attrConverterUint64}
	attrDefs[BTRFS_SEND_A_CLONE_LEN] = attrMapping{"BTRFS_SEND_A_CLONE_LEN", attrConverterUint64}

	// Sanity check (hopefully no holes).
	for i, attr := range attrDefs {
		if i != BTRFS_SEND_A_UNSPEC && attr.converter == nil {
			return nil
		}
	}
	return &attrDefs
}

// do the initialization of the commands mapping
var commandsDefs *[BTRFS_SEND_C_MAX_PLUS_ONE]commandMapOp = initCommandsDefinitions()
var attrDefs *[BTRFS_SEND_A_MAX_V1_PLUS_ONE]attrMapping = initAttributeDefinitions()

// readCommand return a command from reading and parsing the stream input
func readCommand(input *bufio.Reader) (*commandInst, error) {
	cmdSizeB, err := peekAndDiscard(input, 4)
	if err != nil {
		return nil, fmt.Errorf("short read on command size: %v", err)
	}
	cmdSize := binary.LittleEndian.Uint32(cmdSizeB)
	// debug("command size: '%v' (%v)", cmdSize, cmdSizeB)
	cmdTypeB, err := peekAndDiscard(input, 2)
	if err != nil {
		return nil, fmt.Errorf("short read on command type: %v", err)
	}
	cmdType := binary.LittleEndian.Uint16(cmdTypeB)
	// debug("command type: '%v' (%v)", cmdType, cmdTypeB)
	if cmdType > BTRFS_SEND_C_MAX {
		return nil, fmt.Errorf("stream contains invalid command type %v", cmdType)
	}
	_, err = peekAndDiscard(input, 4)
	if err != nil {
		return nil, fmt.Errorf("short read on command checksum: %v", err)
	}
	cmdData, err := peekAndDiscard(input, int(cmdSize))
	if err != nil {
		return nil, fmt.Errorf("short read on command data: %v", err)
	}
	return &commandInst{
		OriginalType: cmdType,
		Type:         &commandsDefs[cmdType],
		data:         cmdData,
	}, nil
}

// ReadParam return a parameter of a command, if it matches the one expected
func (command *commandInst) ReadParam(expectedType int) (interface{}, error) {
	if len(command.data) < 4 {
		return nil, fmt.Errorf("no more parameters")
	}
	paramType := binary.LittleEndian.Uint16(command.data[0:2])
	// debug("param type: '%v' (expected: %v, raw: %v)", attrDefs[paramType].Name, attrDefs[expectedType].Name, command.data[0:2])
	if int(paramType) != expectedType {
		return nil, fmt.Errorf("expect type %v; got %v", attrDefs[expectedType].Name, attrDefs[paramType].Name)
	}
	paramLength := binary.LittleEndian.Uint16(command.data[2:4])
	// debug("param length: '%v' (raw: %v)", paramLength, command.data[2:4])
	if int(paramLength)+4 > len(command.data) {
		return nil, fmt.Errorf("short command param; length was %v but only %v left", paramLength, len(command.data)-4)
	}

	attr := attrDefs[paramType]
	data := command.data[4 : 4+paramLength]
	converted := attr.converter(data)
	debugInd(1, "param %s [len=%d]: %v", attr.Name, paramLength, converted)

	command.data = command.data[4+paramLength:]
	return converted, nil
}
