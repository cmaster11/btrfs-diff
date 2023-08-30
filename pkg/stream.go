package pkg

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type ProcessFileWithOutputArgs struct {
	ArgFile     string
	IgnorePaths DiffIgnorePaths
	JSON        bool
}

func ProcessFile(fileName string) (*Diff, error) {
	fileName, err := filepath.Abs(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "bad filename")
	}

	f, err := os.Open(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open file")
	}
	defer f.Close()

	diff, err := ProcessBTRFSStream(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed to process btrfs stream file")
	}

	return diff, nil
}

func ProcessFileAndOutput(args *ProcessFileWithOutputArgs) error {
	diff, err := ProcessFile(args.ArgFile)
	if err != nil {
		return errors.Wrap(err, "failed to process file")
	}

	if args.JSON {
		str, err := diff.printJSON(args.IgnorePaths)
		if err != nil {
			return errors.Wrapf(err, "failed to marshal json")
		}
		fmt.Printf("%s", str)
	} else {
		diff.print(args.IgnorePaths)
	}

	return nil
}

func validateBTRFSStream(input *bufio.Reader) error {
	btrfsStreamHeader, err := input.ReadString('\x00')
	if err != nil {
		return errors.Wrap(err, "failed to read stream header")
	}
	if btrfsStreamHeader[:len(btrfsStreamHeader)-1] != BTRFS_SEND_STREAM_MAGIC {
		return errors.Wrapf(err, "bad stream magic data, expected %v got %v", BTRFS_SEND_STREAM_MAGIC, btrfsStreamHeader)
	}
	verB, err := peekAndDiscard(input, 4)
	if err != nil {
		return errors.Wrap(err, "failed to read version bytes")
	}
	ver := binary.LittleEndian.Uint32(verB)
	if ver != 1 {
		return errors.Wrapf(err, "unexpected stream version %v", ver)
	}

	return nil
}

func errUnsupported(command *commandInst) error {
	return errors.Errorf("unsupported command %d %s", command.OriginalType, command.Type.Name)
}

func ProcessBTRFSStream(stream *os.File) (*Diff, error) {
	input := bufio.NewReader(stream)

	if err := validateBTRFSStream(input); err != nil {
		return nil, errors.Wrap(err, "failed to validate btrfs stream")
	}

	diff := &Diff{
		root: &DiffNode{
			NodeType: DiffNodeTypeDir,
			Path:     "",
			Children: make(map[string]*DiffNode),
		},
	}

	var err error
	stop := false
	for {
		if stop {
			break
		}

		var command *commandInst
		command, err = readCommand(input)
		if err != nil {
			return nil, errors.Wrap(err, "failed to read command")
		}

		if command.Type.Op != opIgnore {
			info("cmd: %s, mapped: %s", command.Type.Name, command.Type.Op)
		}

		switch command.Type.Op {
		case opUnspec:
			return nil, errUnsupported(command)
		case opIgnore:
			continue
		case opEnd:
			stop = true
			continue

		case opRename:
			path, err := command.ReadParam(BTRFS_SEND_A_PATH)
			if err != nil {
				return nil, errors.Wrap(err, "failed to read from-path param")
			}

			var fromPath string
			var toPath string

			if command.OriginalType == BTRFS_SEND_C_RENAME {
				_toPath, err := command.ReadParam(BTRFS_SEND_A_PATH_TO)
				if err != nil {
					return nil, errors.Wrap(err, "failed to read to-path param")
				}
				toPath = _toPath.(string)
				fromPath = path.(string)
			} else if command.OriginalType == BTRFS_SEND_C_LINK {
				_fromPath, err := command.ReadParam(BTRFS_SEND_A_PATH_LINK)
				if err != nil {
					return nil, errors.Wrap(err, "failed to read to-path param")
				}
				fromPath = _fromPath.(string)
				toPath = path.(string)
			} else {
				return nil, errors.Wrapf(err, "invalid command for rename: %s", command.Type.Name)
			}

			if err := diff.processRenameOrLink(fromPath, toPath, command); err != nil {
				return nil, errors.Wrap(err, "failed to process rename")
			}
			continue

		case opDelete:
			path, err := command.ReadParam(BTRFS_SEND_A_PATH)
			if err != nil {
				return nil, errors.Wrap(err, "failed to read path param")
			}

			if err := diff.processDelete(path.(string), command); err != nil {
				return nil, errors.Wrap(err, "failed to process delete")
			}
			continue

		default:
			switch command.OriginalType {
			case BTRFS_SEND_C_SNAPSHOT:
				fallthrough
			case BTRFS_SEND_C_SUBVOL:
				path, err := command.ReadParam(BTRFS_SEND_A_PATH)
				if err != nil {
					return nil, errors.Wrap(err, "failed to read path param")
				}
				uuid, err := command.ReadParam(BTRFS_SEND_A_UUID)
				if err != nil {
					return nil, errors.Wrap(err, "failed to read uuid param")
				}
				ctransid, err := command.ReadParam(BTRFS_SEND_A_CTRANSID)
				if err != nil {
					return nil, errors.Wrap(err, "failed to read ctransid param")
				}
				if command.OriginalType == BTRFS_SEND_C_SNAPSHOT {
					cloneUUID, err := command.ReadParam(BTRFS_SEND_A_CLONE_UUID)
					if err != nil {
						return nil, errors.Wrap(err, "failed to read clone uuid param")
					}
					cloneCTransid, err := command.ReadParam(BTRFS_SEND_A_CLONE_CTRANSID)
					if err != nil {
						return nil, errors.Wrap(err, "failed to read clone ctransid param")
					}
					info("received snapshot at %s [uuid=%s,ctransid=%d,clone_uuid=%s,clone_ctransid=%d]", path, uuid, ctransid, cloneUUID, cloneCTransid)
				} else {
					info("received subvol at %s [uuid=%s,ctransid=%d]", path, uuid, ctransid)
				}
				continue

			case BTRFS_SEND_C_CLONE:
				return nil, errUnsupported(command)
			}

			path, err := command.ReadParam(BTRFS_SEND_A_PATH)
			if err != nil {
				return nil, errors.Wrap(err, "failed to read path param")
			}

			// https://docs.huihoo.com/doxygen/linux/kernel/3.7/fs_2btrfs_2send_8c_source.html :3365
			switch command.OriginalType {
			case BTRFS_SEND_C_MKFILE:
				fallthrough
			case BTRFS_SEND_C_MKDIR:
				fallthrough
			case BTRFS_SEND_C_SYMLINK:
				fallthrough
			case BTRFS_SEND_C_MKNOD:
				fallthrough
			case BTRFS_SEND_C_MKFIFO:
				fallthrough
			case BTRFS_SEND_C_MKSOCK:
				if err := diff.processCreate(path.(string), command); err != nil {
					return nil, errors.Wrap(err, "failed to process create")
				}
				continue

			case BTRFS_SEND_C_WRITE:
				fallthrough
			case BTRFS_SEND_C_UPDATE_EXTENT:
				fallthrough
			case BTRFS_SEND_C_TRUNCATE:
				fallthrough
			case BTRFS_SEND_C_CHMOD:
				fallthrough
			case BTRFS_SEND_C_CHOWN:
				fallthrough
			case BTRFS_SEND_C_SET_XATTR:
				fallthrough
			case BTRFS_SEND_C_REMOVE_XATTR:
				fallthrough
			case BTRFS_SEND_C_UTIMES:
				if err := diff.processModify(path.(string), command); err != nil {
					return nil, errors.Wrap(err, "failed to process create")
				}
				continue
			}

			return nil, errors.Errorf("unhandled command %s", command.Type.Name)
		}
	}

	return diff, nil
}

type Diff struct {
	root *DiffNode
}

type DiffIgnorePaths []*regexp.Regexp

func (p DiffIgnorePaths) Matches(f *DiffNode) bool {
	pa := f.GetChainPath()
	for _, re := range p {
		if re.MatchString(pa) {
			return true
		}
	}
	return false
}

type DiffJSONStruct struct {
	Added   []*DiffNode `json:"added"`
	Changed []*DiffNode `json:"changed"`
	Deleted []*DiffNode `json:"deleted"`
}

func shouldPrintNode(n *DiffNode) bool {
	if n.isBTRFSTemporaryNode() {
		return false
	}
	if n.State == opCreate || n.State == opModify || n.State == opDelete {
		return true
	}
	return false
}

func (d *Diff) print(ignorePaths DiffIgnorePaths) {
	info("=== Tree ===")
	d.root.traverse(func(f *DiffNode) {
		if ignorePaths.Matches(f) {
			return
		}

		if shouldPrintNode(f) {
			info(f.String())
		}

		if f.DeletedInSnapshot && f.State != opDelete {
			info(f.StringForDeleted())
		}
	})
}

func (d *Diff) GetDiffStruct(ignorePaths DiffIgnorePaths) *DiffJSONStruct {
	s := &DiffJSONStruct{}

	d.root.traverse(func(f *DiffNode) {
		if ignorePaths.Matches(f) {
			return
		}

		if !shouldPrintNode(f) {
			return
		}

		if f.State == opCreate {
			s.Added = append(s.Added, f)
		} else if f.State == opDelete {
			s.Deleted = append(s.Deleted, f)
		} else {
			s.Changed = append(s.Changed, f)
		}

		if f.DeletedInSnapshot && f.State != opDelete {
			s.Deleted = append(s.Deleted, f)
		}
	})

	return s
}

func (d *Diff) printJSON(ignorePaths DiffIgnorePaths) (string, error) {
	s := d.GetDiffStruct(ignorePaths)

	b, err := json.Marshal(s)
	if err != nil {
		return "", errors.Wrap(err, "failed to marshal diff")
	}
	return string(b), nil
}

func (d *Diff) getNodeByPath(path string) *DiffNode {
	entries := strings.Split(path, "/")
	if entries[0] == "" {
		entries = entries[1:]
	}
	var ok bool
	currentNode := d.root
	for _, entry := range entries {
		currentNode, ok = currentNode.Children[entry]
		if !ok {
			return nil
		}
	}

	return currentNode
}

func getLastPathPart(path string) string {
	parts := strings.Split(path, "/")
	return parts[len(parts)-1]
}

func (d *Diff) getNodeParentOrMkdir(path string) *DiffNode {
	parts := strings.Split(path, "/")
	parentPath := strings.Join(parts[:len(parts)-1], "/")
	if parentPath == "" {
		return d.root
	}
	return d.root.mkdirp(parentPath, false, false)
}

func (d *Diff) processCreate(path string, command *commandInst) error {
	node := d.getNodeByPath(path)
	if node != nil {
		return errors.Errorf("found existing node in tree while processing create operation")
	}

	var nodeType DiffNodeType
	switch command.OriginalType {
	case BTRFS_SEND_C_MKFILE:
		nodeType = DiffNodeTypeFile
	case BTRFS_SEND_C_MKDIR:
		nodeType = DiffNodeTypeDir
	case BTRFS_SEND_C_SYMLINK:
		nodeType = DiffNodeTypeSymLink
	case BTRFS_SEND_C_MKNOD:
		nodeType = DiffNodeTypeNode
	case BTRFS_SEND_C_MKFIFO:
		nodeType = DiffNodeTypeFIFO
	case BTRFS_SEND_C_MKSOCK:
		nodeType = DiffNodeTypeSock
	default:
		return errors.Errorf("unsupported command for create operation: %s", command.Type.Name)
	}

	if nodeType == DiffNodeTypeDir {
		node = d.root.mkdirp(path, false, true)
	} else {
		parent := d.getNodeParentOrMkdir(path)
		node = &DiffNode{
			Path:     getLastPathPart(path),
			NodeType: nodeType,
			State:    opCreate,
		}
		if err := parent.addNode(node); err != nil {
			return errors.Wrapf(err, "failed to add node %s to parent %s", node.Path, parent.GetChainPath())
		}
	}

	if command.OriginalType == BTRFS_SEND_C_SYMLINK {
		{
			_, err := command.ReadParam(BTRFS_SEND_A_INO)
			if err != nil {
				return errors.Wrap(err, "failed to read ino link param")
			}
		}

		pathLink, err := command.ReadParam(BTRFS_SEND_A_PATH_LINK)
		if err != nil {
			return errors.Wrap(err, "failed to read path link param")
		}

		// Links can have relative paths!

		linkDestination := d.getNodeByPath(pathLink.(string))
		if linkDestination == nil {
			info("link %s destination not found", pathLink.(string))
			// NOTE: we CANNOT add this to the tree as of now, relative paths and so on to deal with
			linkDestination = &DiffNode{
				NodeType: DiffNodeTypeUnknown,
				Path:     pathLink.(string),
				Children: make(map[string]*DiffNode),
			}
		}

		node.Relations = append(node.Relations, &DiffNodeRelation{linkDestination, DiffNodeReasonLinkDest})
	}

	info("created %s [type=%s]", path, node.NodeType)
	return nil
}

func (d *Diff) processModify(path string, command *commandInst) error {
	node := d.getNodeByPath(path)
	if node == nil {
		parent := d.getNodeParentOrMkdir(path)
		node = &DiffNode{
			Path:     getLastPathPart(path),
			NodeType: DiffNodeTypeUnknown,
			Children: make(map[string]*DiffNode),
		}
		if err := parent.addNode(node); err != nil {
			return errors.Wrapf(err, "failed to add node %s to parent %s", node.Path, parent.GetChainPath())
		}
	}

	if node.State != opCreate {
		node.State = opModify
	}

	switch command.OriginalType {
	case BTRFS_SEND_C_WRITE:
		offset, err := command.ReadParam(BTRFS_SEND_A_FILE_OFFSET)
		if err != nil {
			return errors.Wrap(err, "failed to read write offset param")
		}
		sentData, err := command.ReadParam(BTRFS_SEND_A_DATA)
		if err != nil {
			return errors.Wrap(err, "failed to read written data param")
		}

		dataLen := uint64(len(sentData.(*bytesData).bytes))
		if len(node.Changes) > 0 {
			lastChange := node.Changes[len(node.Changes)-1]
			// Concat multiple writes
			if strings.HasPrefix(lastChange, "write") && node.lastDataWrittenOffset+node.lastDataWrittenLen == offset {
				node.Changes = node.Changes[:len(node.Changes)-1]
				offset = node.lastDataWrittenOffset
				dataLen = dataLen + node.lastDataWrittenLen
			}
		}

		if node.NodeType == DiffNodeTypeUnknown {
			node.NodeType = DiffNodeTypeFile
		}
		node.Changes = append(node.Changes, fmt.Sprintf("write:offset=%d:data_len=%d", offset, dataLen))
		node.lastDataWrittenOffset = offset.(uint64)
		node.lastDataWrittenLen = dataLen
		info("modified: write at %s at %v: %s", path, offset, sentData)
	case BTRFS_SEND_C_UPDATE_EXTENT:
		offset, err := command.ReadParam(BTRFS_SEND_A_FILE_OFFSET)
		if err != nil {
			return errors.Wrap(err, "failed to read write offset param")
		}
		size, err := command.ReadParam(BTRFS_SEND_A_SIZE)
		if err != nil {
			return errors.Wrap(err, "failed to read written size param")
		}

		dataLen := size.(uint64)
		if len(node.Changes) > 0 {
			lastChange := node.Changes[len(node.Changes)-1]
			// Concat multiple writes
			if strings.HasPrefix(lastChange, "write") && node.lastDataWrittenOffset+node.lastDataWrittenLen == offset {
				node.Changes = node.Changes[:len(node.Changes)-1]
				offset = node.lastDataWrittenOffset
				dataLen = dataLen + node.lastDataWrittenLen
			}
		}

		if node.NodeType == DiffNodeTypeUnknown {
			node.NodeType = DiffNodeTypeFile
		}
		node.Changes = append(node.Changes, fmt.Sprintf("write:offset=%d:data_len=%d", offset, dataLen))
		node.lastDataWrittenOffset = offset.(uint64)
		node.lastDataWrittenLen = dataLen
		info("modified: write (extent) at %s at %v", path, offset)
	case BTRFS_SEND_C_TRUNCATE:
		size, err := command.ReadParam(BTRFS_SEND_A_SIZE)
		if err != nil {
			return errors.Wrap(err, "failed to read size param")
		}

		if node.NodeType == DiffNodeTypeUnknown {
			node.NodeType = DiffNodeTypeFile
		}
		node.Changes = append(node.Changes, fmt.Sprintf("truncate:size=%d", size))
		info("modified: trucate at %s [size=%d]", path, size)
	case BTRFS_SEND_C_UTIMES:
		atime, err := command.ReadParam(BTRFS_SEND_A_ATIME)
		if err != nil {
			return errors.Wrap(err, "failed to read atime param")
		}
		mtime, err := command.ReadParam(BTRFS_SEND_A_MTIME)
		if err != nil {
			return errors.Wrap(err, "failed to read mtime param")
		}
		ctime, err := command.ReadParam(BTRFS_SEND_A_CTIME)
		if err != nil {
			return errors.Wrap(err, "failed to read ctime param")
		}

		node.Changes = append(node.Changes, fmt.Sprintf("utime:atime=%s,mtime=%s,ctime=%s", atime, mtime, ctime))
		info("modified: utimes at %s [atime=%s,mtime=%s,ctime=%s]", path, atime, mtime, ctime)
	case BTRFS_SEND_C_CHMOD:
		mode, err := command.ReadParam(BTRFS_SEND_A_MODE)
		if err != nil {
			return errors.Wrap(err, "failed to read mode param")
		}
		node.Changes = append(node.Changes, fmt.Sprintf("chmod:mode=%o", mode))
		info("modified: chmod at %s [chmod=%o]", path, mode)
	case BTRFS_SEND_C_CHOWN:
		uid, err := command.ReadParam(BTRFS_SEND_A_UID)
		if err != nil {
			return errors.Wrap(err, "failed to read uid param")
		}
		gid, err := command.ReadParam(BTRFS_SEND_A_GID)
		if err != nil {
			return errors.Wrap(err, "failed to read gid param")
		}
		node.Changes = append(node.Changes, fmt.Sprintf("chown:uid=%d,gid=%d", uid, gid))
		info("modified: chown at %s [uid=%d,gid=%d]", path, uid, gid)
	case BTRFS_SEND_C_SET_XATTR:
		xattrName, err := command.ReadParam(BTRFS_SEND_A_XATTR_NAME)
		if err != nil {
			return errors.Wrap(err, "failed to read xattrName param")
		}
		xattrData, err := command.ReadParam(BTRFS_SEND_A_XATTR_DATA)
		if err != nil {
			return errors.Wrap(err, "failed to read xattrData param")
		}
		node.Changes = append(node.Changes, fmt.Sprintf("set_xattr:name=%s,data=%v", xattrName, xattrData))
		info("modified: set xattr at %s [name=%s,data=%v]", path, xattrName, xattrData)
	case BTRFS_SEND_C_REMOVE_XATTR:
		xattrName, err := command.ReadParam(BTRFS_SEND_A_XATTR_NAME)
		if err != nil {
			return errors.Wrap(err, "failed to read xattrName param")
		}
		node.Changes = append(node.Changes, fmt.Sprintf("remove_xattr:name=%s", xattrName))
		info("modified: remove xattr at %s [name=%s]", path, xattrName)
	default:
		return errors.Errorf("unhandled modify command %s", command.Type.Name)
	}
	return nil
}

// These are the tmp nodes generated before actual inode linking
// They only create noise
var regexNewNode = regexp.MustCompile(`o\d+-\d+-\d+`)

func (d *Diff) processRenameOrLink(from, to string, command *commandInst) error {
	pathFromIsNewNode := regexNewNode.MatchString(from)

	nodeSrc := d.getNodeByPath(from)
	if nodeSrc == nil && !pathFromIsNewNode {
		// Create a fake node as source
		nodeSrc = &DiffNode{
			NodeType: DiffNodeTypeUnknown,
			Path:     getLastPathPart(from),
			Children: make(map[string]*DiffNode),
		}

		if command.OriginalType == BTRFS_SEND_C_RENAME {
			parent := d.getNodeParentOrMkdir(to)
			if err := parent.addNode(nodeSrc); err != nil {
				return errors.Wrapf(err, "failed to add fake node %s to parent %s", nodeSrc.GetChainPath(), parent.GetChainPath())
			}
		}
	}

	if nodeSrc == nil {
		debug("could not found source node %s for %s command", from, command.Type.Name)
	}

	nodeType := DiffNodeTypeUnknown
	var relations []*DiffNodeRelation
	var children = make(map[string]*DiffNode)

	if nodeSrc != nil {
		nodeType = nodeSrc.NodeType
		relations = nodeSrc.Relations
		for key, val := range nodeSrc.Children {
			children[key] = val
		}

		// Only mark the source node as deleted if there is a rename.
		// A link will preserve the original node
		if command.OriginalType == BTRFS_SEND_C_RENAME {
			if err := d.processDelete(from, command); err != nil {
				return errors.Wrapf(err, "failed to delete rename source node at path %s", from)
			}
		}

		if !pathFromIsNewNode {
			if command.OriginalType == BTRFS_SEND_C_RENAME {
				relations = append(relations, &DiffNodeRelation{nodeSrc, DiffNodeReasonRenameSrc})
			} else if command.OriginalType == BTRFS_SEND_C_LINK {
				relations = append(relations, &DiffNodeRelation{nodeSrc, DiffNodeReasonLinkDest})
			}
		}
	}

	parent := d.getNodeParentOrMkdir(to)
	nodeTo := &DiffNode{
		NodeType:  nodeType,
		Path:      getLastPathPart(to),
		Relations: relations,
		Children:  children,
		State:     opCreate,
	}
	if err := parent.addNode(nodeTo); err != nil {
		return errors.Wrapf(err, "failed to add node %s to renamed node destination parent %s", nodeSrc.GetChainPath(), parent.GetChainPath())
	}
	if nodeSrc != nil {
		if command.OriginalType == BTRFS_SEND_C_RENAME {
			nodeSrc.Relations = append(nodeSrc.Relations, &DiffNodeRelation{nodeTo, DiffNodeReasonRenameDest})
		}
	}

	info("rename from %s to %s", from, to)
	return nil
}

func (d *Diff) processDelete(path string, command *commandInst) error {
	node := d.getNodeByPath(path)
	if node == nil {
		// Create a fake node as source
		node = &DiffNode{
			NodeType: DiffNodeTypeUnknown,
			Path:     getLastPathPart(path),
			Children: make(map[string]*DiffNode),
		}

		parent := d.getNodeParentOrMkdir(path)
		if err := parent.addNode(node); err != nil {
			return errors.Wrapf(err, "failed to add deleted node %s to parent %s", node.GetChainPath(), parent)
		}
	}

	if node.NodeType == DiffNodeTypeUnknown {
		if command.OriginalType == BTRFS_SEND_C_RMDIR {
			node.NodeType = DiffNodeTypeDir
		}
	}

	node.State = opDelete
	node.DeletedInSnapshot = true

	// If the node parent is a btrfs temporary folder, then move this file under the rightful owner
	if regexNewNode.MatchString(node.Parent.Path) {
		renameSrc := node.Parent.followRenameChainSrc()
		if renameSrc != nil {
			if nodeInSrc, ok := renameSrc.Children[node.Path]; ok {
				// We just mark that node as deleted in this snapshot and treat the current node as never existed
				nodeInSrc.DeletedInSnapshot = true

				if command.OriginalType == BTRFS_SEND_C_RMDIR {
					nodeInSrc.NodeType = DiffNodeTypeDir
				}

				if err := node.removeFromParent(); err != nil {
					return errors.Wrapf(err, "failed to remove deleted node (ignored) %s from parent", node)
				}
				return nil
			}

			if err := renameSrc.addNode(node); err != nil {
				return errors.Wrapf(err, "failed to move deleted node %s to rename chain source %s", node.GetChainPath(), renameSrc)
			}
		}
	}

	info("deleted %s", node)
	return nil
}
