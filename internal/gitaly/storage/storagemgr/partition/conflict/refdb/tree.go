package refdb

import (
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
)

// children contains child nodes of a node keyed by their
// path component.
type children map[string]*node

// node is a node in the reference DB. Each node models single
// path segment. For example, `refs/heads/main` would be split
// into `refs`, `heads` and `main` components.
type node struct {
	// target is the reference's target value.
	target string

	// children are the child nodes of this node.
	children children
	// childReferences counts live references below this
	// node. If the node has live references below it,
	// it can't become a reference as it would lead to a
	// directory-file conflict.
	childReferences uint
}

// newNode returns a new node.
func newNode() *node {
	return &node{children: children{}}
}

// clone returns a deep copy of the node.
func (n *node) clone() *node {
	children := make(children, len(n.children))
	for key, child := range n.children {
		children[key] = child.clone()
	}

	return &node{
		target:          n.target,
		childReferences: n.childReferences,
		children:        children,
	}
}

// isReference returns whether this node models a reference.
func (n *node) isReference(zeroOID git.ObjectID) bool {
	return n.isModified() && n.target != zeroOID.String()
}

// isModified returns true if the node has been modified by a write in the history.
func (n *node) isModified() bool {
	return n.target != ""
}

func (n *node) applyUpdates(zeroOID git.ObjectID, updates git.ReferenceUpdates) error {
	for ref, update := range updates {
		if err := n.applyUpdate(zeroOID, ref, update); err != nil {
			return err
		}
	}

	return nil
}

func (n *node) applyUpdate(zeroOID git.ObjectID, ref git.ReferenceName, update git.ReferenceUpdate) error {
	var (
		refPrefix   string
		refBase     = ref.String()
		parentNodes []*node
		node        = n
	)

	// Walk down the tree until we find the node of the reference.
	for {
		prefix, suffix, foundSeparator := strings.Cut(refBase, "/")
		if !foundSeparator {
			// If there was no separator, then it means the current node
			// is the parent node of the reference that is modified.
			//
			// Get the target node of the reference write.
			targetNode := node.children[refBase]
			if targetNode == nil {
				// The target node didn't exist yet so create it.
				targetNode = newNode()
				node.children[refBase] = targetNode
			}

			parentNodes = append(parentNodes, node)
			node = targetNode
			break
		}

		// Since there was a separator, we still need to walk down the tree
		// to find parent node.
		child := node.children[prefix]
		if child == nil {
			// Child node didn't exist. Create it so we can walk it further down.
			child = newNode()
			node.children[prefix] = child
		}

		currentRef := filepath.Join(refPrefix, prefix)
		if child.isReference(zeroOID) {
			// We can only create a reference if none of its parents already exist.
			// Doing so would lead to a directory-file conflict with loose references.
			//
			// reftable format would support storing both `refs/heads/main` and
			// `refs/heads/main/child` but prevents this for consistency with the files
			// backend.
			return NewParentReferenceExistsError(git.ReferenceName(currentRef), ref)
		}

		parentNodes = append(parentNodes, node)
		node = child
		refPrefix = currentRef
		refBase = suffix

	}

	// Figure out whether this is a symbolic reference update or not.
	expectedOldValue, newValue := update.OldOID.String(), update.NewOID.String()
	if !update.IsRegularUpdate() {
		expectedOldValue, newValue = update.OldTarget.String(), update.NewTarget.String()
	}

	// Check if there are live references below this node in the hierarchy.
	// If so, we can't create this reference as it would lead to a conflict
	// where the both `refs/heads/main` and `refs/heads/main/child`. This
	// is not allowed as with the files backend `refs/heads/main` can't be
	// both a file and a directory in the loose reference store.
	//
	// reftables follow suit to maintain consistency with the files backend.
	if node.childReferences > 0 {
		return NewChildReferencesExistError(ref)
	}

	// Check whether the reference's old value matches the expected.
	//
	// If the reference has not been modified, there has been no concurrent changes to this
	// reference. We don't need to perform a conflict check as the transaction would have
	// verified the update in the snapshot before staging this update. The check there
	// is still valid since no one has modified this reference concurrently.
	if node.isModified() && node.target != expectedOldValue {
		return NewUnexpectedOldValueError(ref, expectedOldValue, node.target)
	}

	isCreation := !node.isReference(zeroOID) && newValue != zeroOID.String()
	isDeletion := node.isReference(zeroOID) && newValue == zeroOID.String()

	node.target = newValue

	// Update the reference counters of the parent nodes to accurately reflect the created
	// or deleted child reference.
	if isCreation {
		for _, node := range parentNodes {
			node.childReferences++
		}
	} else if isDeletion {
		for _, node := range parentNodes {
			node.childReferences--
		}
	}

	return nil
}

func (n *node) evict(zeroOID git.ObjectID, refs map[git.ReferenceName]struct{}) {
	type pathElement struct {
		node  *node
		child string
	}

	var parentNodes []pathElement
	for ref := range refs {
		var (
			refPrefix   string
			refBase     = ref.String()
			parentNodes = parentNodes[:0]
			node        = n
		)

		for {
			prefix, suffix, foundSeparator := strings.Cut(refBase, "/")
			if !foundSeparator {
				// If there was no separator, then it means the current node
				// is the parent node of the reference being evicted.
				parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
				node = node.children[refBase]
				break
			}

			// Since there was a separator, we still need to walk down the tree
			// to find the parent node of the reference.
			parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
			node = node.children[prefix]
			refPrefix = filepath.Join(refPrefix, prefix)
			refBase = suffix
		}

		isDeletion := node.isReference(zeroOID)

		// Mark the reference as unmodified.
		node.target = ""

		// Walk the hierarchy upwards from the first parent of the reference node.
		for i := len(parentNodes) - 1; i >= 0; i-- {
			path := parentNodes[i]

			// If the dropped reference was live, update the counter in the parent nodes to
			// reflect the removal of a live reference node below them.
			if isDeletion {
				path.node.childReferences--
			}

			// If the child has no children itself, and it itself doesn't represent a reference
			// modification, the node is unneeded. No write under it would conflict. Remove the node
			// as it is no longer needed for conflict checking.
			if child := path.node.children[path.child]; len(child.children) == 0 && !child.isModified() {
				delete(path.node.children, path.child)
			}
		}
	}
}
