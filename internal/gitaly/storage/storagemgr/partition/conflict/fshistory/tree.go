package fshistory

import (
	"fmt"
	"path/filepath"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v18/internal/gitaly/storage"
)

// children contains child nodes of a node keyed by their
// path component.
type children map[string]*node

type nodeType int

const (
	// negativeNode represents a removed directory entry.
	negativeNode nodeType = iota
	// directoryNode represents a directory.
	directoryNode
	// fileNode represents a file.
	fileNode
)

// node is a node in the tree of file system operations performed.
// Each node maps to a single component of a path, ie. `refs/heads/main`
// maps to <root> -> `refs` -> `heads` -> `main`.
type node struct {
	// nodeType is the type of the node.
	nodeType nodeType
	// writeLSN stores the LSN this node was last modified at.
	writeLSN storage.LSN
	// children are the child nodes of this node.
	children children
	// directoryEntries counts the number of directory
	// entries this node has if it is a directory. Non-empty
	// directories can't be deleted.
	directoryEntries uint
}

// newNode returns a new node.
func newNode(nt nodeType) *node {
	return &node{
		nodeType: nt,
		children: children{},
	}
}

// isDirectory returns whether this node is a directory.
func (n *node) isDirectory() bool {
	return n.nodeType == directoryNode
}

// clone returns a deep copy of the node.
func (n *node) clone() *node {
	children := make(children, len(n.children))
	for key, child := range n.children {
		children[key] = child.clone()
	}

	return &node{
		nodeType:         n.nodeType,
		writeLSN:         n.writeLSN,
		directoryEntries: n.directoryEntries,
		children:         children,
	}
}

// isLive returns whether this node represents a live directory entry.
func (n *node) isLive() bool {
	return n.nodeType != negativeNode
}

func (tx *Transaction) applyUpdate(path string, newType nodeType) error {
	var (
		pathPrefix string
		pathBase   = path
		parentNode = tx.root
	)

	// Walk down the tree until we find the parent node.
	for {
		prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
		if !foundSeparator {
			// If there was no separator, then it means the current node
			// is the parent directory of the target node.
			break
		}

		// Since there was a separator, we still need to walk down the tree
		// to find parent node.
		child := parentNode.children[prefix]
		if child == nil {
			// Child node didn't exist. Create it so we can walk it further down.
			// The node must be a directory as otherwise the transaction wouldn't
			// attempt to operate in it.
			child = newNode(directoryNode)
			parentNode.children[prefix] = child
			parentNode.directoryEntries++
		}

		currentPath := filepath.Join(pathPrefix, prefix)
		if !child.isDirectory() {
			// This node was not a directory and can't be walked down.
			return newNotDirectoryError(currentPath)
		}

		parentNode = child
		pathPrefix = currentPath
		pathBase = suffix
	}

	node := parentNode.children[pathBase]
	if node == nil {
		// The target node didn't exist yet so create it. As we had no
		// record of the node yet, this operation can't conflict.
		node = newNode(newType)
		if node.isLive() {
			parentNode.directoryEntries++
		}
		parentNode.children[pathBase] = node
		tx.modifiedNodes[path] = node
		return nil
	}

	switch node.nodeType {
	case negativeNode:
		switch newType {
		case directoryNode, fileNode:
			node.nodeType = newType
			parentNode.directoryEntries++
		case negativeNode:
			return newNotFoundError(path)
		default:
			return fmt.Errorf("unhandled negative operation: %v", newType)
		}
	case directoryNode:
		switch newType {
		case negativeNode:
			// Check whether this node has directory entries. If so, it can't be deleted as
			// it is a non-empty directory.
			if node.directoryEntries > 0 {
				return newDirectoryNotEmptyError(path)
			}

			node.nodeType = newType
			parentNode.directoryEntries--
		case directoryNode, fileNode:
			return newAlreadyExistsError(path)
		default:
			return fmt.Errorf("unhandled directory operation: %v", newType)
		}
	case fileNode:
		switch newType {
		case negativeNode:
			node.nodeType = newType
			parentNode.directoryEntries--
		case directoryNode, fileNode:
			return newAlreadyExistsError(path)
		default:
			return fmt.Errorf("unhandled file operation: %v", newType)
		}
	default:
		return fmt.Errorf("unhandled node type: %v", node.nodeType)
	}

	// Reset the nodes writeLSN. This avoids later reads being considered as conflicting
	// since the node is now what we read. As we don't know the LSN this transaction is
	// going to commit at, we use 0. During commit, we update the LSN to reflect the actual
	// LSN of the committed transaction.
	node.writeLSN = 0

	tx.modifiedNodes[path] = node

	return nil
}

func (tx *Transaction) findNode(path string) (*node, error) {
	var (
		pathPrefix string
		pathBase   = path
		node       = tx.root
	)

	// Walk down the tree until we find the node of the reference.
	for {
		prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
		if !foundSeparator {
			// If there was no separator, then it means the current node
			// is the parent of the target node.
			//
			// Get the target node.
			return node.children[pathBase], nil
		}

		// Since there was a separator, we still need to walk down the tree
		// to find parent node.
		child := node.children[prefix]
		if child == nil {
			// Child node didn't exist. As a parent of the target node doesn't exist,
			// the target can't exist either.
			return nil, nil
		}

		currentPath := filepath.Join(pathPrefix, prefix)
		if tx.readLSN < child.writeLSN {
			// If the child LSN is later than the read, it has been written after
			// our transaction started. This is a potential conflict.
			return nil, NewReadWriteConflictError(currentPath, tx.readLSN, child.writeLSN)
		} else if !child.isDirectory() {
			// This node was not a directory and can't be walked down.
			return nil, newNotDirectoryError(currentPath)
		}

		node = child
		pathPrefix = currentPath
		pathBase = suffix
	}
}

// evict evicts the given paths from the tree if they are no longer needed
// to represent the tree structure.
func (n *node) evict(evictedLSN storage.LSN, paths map[string]struct{}) {
	type pathElement struct {
		node  *node
		child string
	}

	var parentNodes []pathElement
	for path := range paths {
		var (
			pathPrefix  string
			pathBase    = path
			parentNodes = parentNodes[:0]
			node        = n
		)

		for {
			prefix, suffix, foundSeparator := strings.Cut(pathBase, "/")
			if !foundSeparator {
				if node.children[prefix] == nil {
					// The node has been already evicted, possibly as part of eviciting another
					// node from the same LSN.
					return
				}

				// If there was no separator, then it means the current node
				// is the parent node of the reference being evicted.
				parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
				break
			}

			// Since there was a separator, we still need to walk down the tree
			// to find the parent node of the reference.
			parentNodes = append(parentNodes, pathElement{node: node, child: prefix})
			node = node.children[prefix]
			if node == nil {
				// The node has been already evicted, possibly as part of eviciting another
				// node from the same LSN.
				return
			}

			pathPrefix = filepath.Join(pathPrefix, prefix)
			pathBase = suffix
		}

		// Walk the hierarchy upwards from the first parent of the reference node.
		for i := len(parentNodes) - 1; i >= 0; i-- {
			path := parentNodes[i]

			child := path.node.children[path.child]
			// Check if the child node is still needed. The node can be evicted if it has no children and
			// its LSN is less than or equal to the evicted LSN. If the node has children, we'll need to
			// keep until the children are evicted. We'll evict the parent when the last child is evicted.
			// If the node's LSN is later than the evicted LSN, the node is still needed for conflict checking
			// and can't be evicted. Keep the node and its parents.
			if len(child.children) > 0 || evictedLSN < child.writeLSN {
				break
			}

			if child.isLive() {
				// If the dropped node is not a negative, update the counter in the parent nodes to
				// reflect the removal of a live inode below them.
				path.node.directoryEntries--
			}

			// This node was unneeded, remove it.
			delete(path.node.children, path.child)
		}
	}
}
