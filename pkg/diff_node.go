package pkg

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"strings"
)

type DiffNodeType = string

const (
	DiffNodeTypeUnknown DiffNodeType = "UNKNOWN"
	DiffNodeTypeFile    DiffNodeType = "FILE"
	DiffNodeTypeDir     DiffNodeType = "DIR"
	DiffNodeTypeFIFO    DiffNodeType = "FIFO"
	DiffNodeTypeSock    DiffNodeType = "SOCK"
	DiffNodeTypeSymLink DiffNodeType = "SYMLINK"
	DiffNodeTypeNode    DiffNodeType = "NODE"
)

type DiffNodeReason = string

const (
	DiffNodeReasonRenameSrc  DiffNodeReason = "RENAME_SRC"
	DiffNodeReasonRenameDest DiffNodeReason = "RENAME_DEST"
	DiffNodeReasonLinkDest   DiffNodeReason = "LINK_DEST"
)

type DiffNodeRelation struct {
	Node   *DiffNode
	Reason DiffNodeReason
}

type DiffNodeRelationJSON struct {
	Path   string         `json:"path"`
	Reason DiffNodeReason `json:"reason"`
}

func (r *DiffNodeRelation) MarshalJSON() ([]byte, error) {
	return json.Marshal(&DiffNodeRelationJSON{r.Node.GetChainPath(), r.Reason})
}

type DiffNode struct {
	NodeType DiffNodeType
	Path     string
	State    operation
	//Data  []byte
	// E.g. for a rename will contain the previous state
	Relations         []*DiffNodeRelation
	Changes           []string
	Parent            *DiffNode
	Children          map[string]*DiffNode
	DeletedInSnapshot bool

	// Tmp storage to help logs
	lastDataWrittenOffset uint64
	lastDataWrittenLen    uint64
}

type DiffNodeJSON struct {
	NodeType  DiffNodeType        `json:"node_type"`
	Path      string              `json:"path"`
	State     operation           `json:"state"`
	Relations []*DiffNodeRelation `json:"relations"`
	Changes   []string            `json:"changes"`
}

func (n *DiffNode) MarshalJSON() ([]byte, error) {
	return json.Marshal(&DiffNodeJSON{n.NodeType, n.GetChainPath(), n.State, n.Relations, n.Changes})
}

func (n *DiffNode) isBTRFSTemporaryNode() bool {
	if n.Parent != nil && n.Parent == n.root() && regexNewNode.MatchString(n.Path) {
		return true
	}
	return false
}

func (n *DiffNode) GetChainPath() string {
	if n.Parent != nil {
		return fmt.Sprintf("%s/%s", n.Parent.GetChainPath(), n.Path)
	}

	return fmt.Sprintf("%s", n.Path)
}

func (n *DiffNode) root() *DiffNode {
	if n.Parent != nil {
		return n.Parent.root()
	}

	return n
}

func (n *DiffNode) StringForDeleted() string {
	p := n.GetChainPath()
	if p == "" {
		p = "/"
	}

	var parts []string

	parts = append(parts, fmt.Sprintf("[%s][%s]", n.NodeType, opDelete))
	parts = append(parts, p)

	return strings.Join(parts, " ")
}

func (n *DiffNode) String() string {
	p := n.GetChainPath()
	if p == "" {
		p = "/"
	}

	var parts []string

	parts = append(parts, fmt.Sprintf("[%s][%s]", n.NodeType, n.State.String()))
	parts = append(parts, p)

	for _, r := range n.Relations {
		parts = append(parts, fmt.Sprintf("[rel=%s:%s]", r.Node.GetChainPath(), r.Reason))
	}

	for _, r := range n.Changes {
		parts = append(parts, fmt.Sprintf("[change=%s]", r))
	}

	return strings.Join(parts, " ")
}

func (n *DiffNode) traverse(traverseFn func(node *DiffNode)) {
	for _, val := range n.Children {
		traverseFn(val)
		val.traverse(traverseFn)
	}
}

func (n *DiffNode) findRelation(reason DiffNodeReason) *DiffNodeRelation {
	for _, rel := range n.Relations {
		if rel.Reason == reason {
			return rel
		}
	}
	return nil
}

func (n *DiffNode) followRenameChainSrc() *DiffNode {
	if rel := n.findRelation(DiffNodeReasonRenameSrc); rel != nil {
		return rel.Node.followRenameChainSrc()
	}
	return n
}

func (n *DiffNode) mkdirp(path string, oldNodesAreCreatedInSnapshot bool, newNodesAreCreatedInSnapshot bool) *DiffNode {
	entries := strings.Split(path, "/")
	if entries[0] == "" {
		entries = entries[1:]
	}
	currentNode := n
	entriesLen := len(entries)
	for idx, entry := range entries {
		existing, ok := currentNode.Children[entry]
		if ok {
			currentNode = existing
			//if followRenameChain {
			//	currentNode = currentNode.followRenameChainSrc()
			//}
		} else {
			state := opUnspec
			createdInSnapshot := oldNodesAreCreatedInSnapshot
			if idx == entriesLen-1 {
				createdInSnapshot = newNodesAreCreatedInSnapshot
			}
			if createdInSnapshot {
				state = opCreate
			}
			newNode := &DiffNode{
				NodeType: DiffNodeTypeDir,
				Path:     entry,
				Parent:   currentNode,
				Children: make(map[string]*DiffNode),
				State:    state,
			}
			currentNode.Children[entry] = newNode
			currentNode = newNode
			debug("created intermediate dir node %s", currentNode)
		}
	}
	return currentNode
}

func (n *DiffNode) addNode(node *DiffNode) error {
	if n.NodeType == DiffNodeTypeUnknown {
		n.NodeType = DiffNodeTypeDir
	}

	existingNode, alreadyExists := n.Children[node.Path]
	if alreadyExists {
		// If we have an already existing node, the only reason for this to be is that
		// the existing node is a deleted one, in which case we can override it completely.
		if existingNode.State != opDelete {
			return errors.Errorf("found existing children node %s while adding new node", node.Path)
		}
	}
	if node.Parent != nil {
		if err := node.Parent.deleteNode(node); err != nil {
			return errors.Wrapf(err, "failed to delete node %s from its parent %s", node.GetChainPath(), node.Parent.GetChainPath())
		}
	}

	node.Parent = n

	if existingNode != nil {
		if err := existingNode.removeFromParent(); err != nil {
			return errors.Wrapf(err, "failed to delete fake existing node %s from its parent %s", existingNode.GetChainPath(), existingNode.Parent.GetChainPath())
		}

		// Merge the fake node into the new one
		// NOTE: this will break links references :( TODO maybe not?
		node.Relations = append(node.Relations, existingNode.Relations...)
		for _, val := range existingNode.Children {
			if err := node.addNode(val); err != nil {
				return errors.Wrapf(err, "failed to move deleted fake node %s children %s to really deleted node %s", existingNode.GetChainPath(), val.GetChainPath(), node.GetChainPath())
			}
		}
		node.DeletedInSnapshot = true

		n.Children[node.Path] = node
		debug("replaced existing deleted node %s with new node %s in parent %s", existingNode, node, n)
		return nil
	}

	n.Children[node.Path] = node
	debug("added node %s to parent %s", node, n)
	return nil
}

func (n *DiffNode) deleteNode(node *DiffNode) error {
	var key string
	for k, v := range n.Children {
		if v == node {
			key = k
			break
		}
	}
	if key == "" {
		return errors.Errorf("child node %s not found in parent %s", node.GetChainPath(), node.Parent.GetChainPath())
	}
	delete(n.Children, key)
	node.Parent = nil
	return nil
}

func (n *DiffNode) removeFromParent() error {
	if n.Parent != nil {
		parent := n.Parent
		if err := parent.deleteNode(n); err != nil {
			return errors.Errorf("failed to remove node %s from parent %s", n.GetChainPath(), n.Parent.GetChainPath())
		}
		debug("deleted node %s from parent %s", n, parent)
	}
	return nil
}
