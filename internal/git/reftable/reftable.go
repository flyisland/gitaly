package reftable

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// NameRegex is a regex for matching reftable names
// e.g. 0x000000000001-0x00000000000a-b54f3b59.ref would result in the following submatches:
//   - 000000000001 (UpdateIndexMin)
//   - 00000000000a (UpdateIndexMax)
//
// See the reftable documentation at https://www.git-scm.com/docs/reftable#_layout for more
// information.
var NameRegex = regexp.MustCompile("0x([[:xdigit:]]{12})-0x([[:xdigit:]]{12})-[0-9a-zA-Z]{8}.ref")

type header struct {
	Name           [4]byte
	Version        uint8
	BlockSize      [3]byte
	MinUpdateIndex uint64
	MaxUpdateIndex uint64
	// HashID is only present if version is 2
	HashID [4]byte
}

type footerBase struct {
	Name           [4]byte
	Version        uint8
	BlockSize      [3]byte
	MinUpdateIndex uint64
	MaxUpdateIndex uint64
}

type footerEnd struct {
	RefIndexOffset     uint64
	ObjectOffsetAndLen uint64
	ObjectIndexOffset  uint64
	LogOffset          uint64
	LogIndexPosition   uint64
	CR32               uint32
}

type footer struct {
	footerBase
	HashID [4]byte
	footerEnd
}

type block struct {
	BlockStart    uint
	FullBlockSize uint
	HeaderOffset  uint
	RestartCount  uint16
	RestartStart  uint
}

type reftable struct {
	blockSize  *uint
	headerSize uint
	footerSize uint
	size       uint
	src        []byte
	header     *header
	footer     *footer
}

// shaFormat maps reftable sha format to Gitaly's hash object.
func (t *reftable) shaFormat() git.ObjectHash {
	if t.footer.Version == 2 && bytes.Equal(t.footer.HashID[:], []byte("s256")) {
		return git.ObjectHashSHA256
	}
	return git.ObjectHashSHA1
}

// parseBlockSize parses the table's header for the block size.
func (t *reftable) parseBlockSize() uint {
	if t.blockSize == nil {
		blockSize := uint(big.NewInt(0).SetBytes(t.header.BlockSize[:]).Uint64())
		t.blockSize = &blockSize
	}

	return *t.blockSize
}

// getBlockRange provides the abs block range if the block is smaller
// than the table.
func (t *reftable) getBlockRange(offset, size uint) (uint, uint) {
	if offset >= t.size {
		return 0, 0
	}

	if offset+size > t.size {
		size = t.size - offset
	}

	return offset, offset + size
}

// extractBlockLen extracts the block length from a given location.
func (t *reftable) extractBlockLen(blockStart uint) uint {
	return uint(big.NewInt(0).SetBytes(t.src[blockStart+1 : blockStart+4]).Uint64())
}

// getVarInt parses a variable int and increases the index.
func (t *reftable) getVarInt(start uint, blockEnd uint) (uint, uint, error) {
	var val uint

	val = uint(t.src[start]) & 0x7f

	for (uint(t.src[start]) & 0x80) > 0 {
		start++
		if start > blockEnd {
			return 0, 0, fmt.Errorf("exceeded block length")
		}

		val = ((val + 1) << 7) | (uint(t.src[start]) & 0x7f)
	}

	return start + 1, val, nil
}

// getRefsFromBlock provides the ref udpates from a reference block.
func (t *reftable) getRefsFromBlock(b *block) ([]git.Reference, error) {
	var references []git.Reference

	prefix := ""

	// Skip the block_type and block_len
	idx := b.BlockStart + 4

	for idx < b.RestartStart {
		var prefixLength, suffixLength, updateIndexDelta uint
		var err error

		idx, prefixLength, err = t.getVarInt(idx, b.RestartStart)
		if err != nil {
			return nil, fmt.Errorf("getting prefix length: %w", err)
		}

		idx, suffixLength, err = t.getVarInt(idx, b.RestartStart)
		if err != nil {
			return nil, fmt.Errorf("getting suffix length: %w", err)
		}

		extra := (suffixLength & 0x7)
		suffixLength >>= 3

		refname := prefix[:prefixLength] + string(t.src[idx:idx+suffixLength])
		idx = idx + suffixLength

		idx, updateIndexDelta, err = t.getVarInt(idx, b.FullBlockSize)
		if err != nil {
			return nil, fmt.Errorf("getting update index delta: %w", err)
		}
		// we don't use this for now
		_ = updateIndexDelta

		reference := git.Reference{
			Name: git.ReferenceName(refname),
		}

		switch extra {
		case 0:
			// Deletion, no value
			reference.Target = t.shaFormat().ZeroOID.String()
		case 1:
			// Regular reference
			hashSize := t.shaFormat().Hash().Size()
			reference.Target = git.ObjectID(hex.EncodeToString(t.src[idx : idx+uint(hashSize)])).String()

			idx += uint(hashSize)
		case 2:
			// Peeled Tag
			hashSize := t.shaFormat().Hash().Size()
			reference.Target = git.ObjectID(hex.EncodeToString(t.src[idx : idx+uint(hashSize)])).String()

			idx += uint(hashSize)

			// For now we don't need the peeledOID, but we still need
			// to skip the index.
			// peeledOID := ObjectID(bytesToHex(t.src[idx : idx+uint(hashSize)]))
			idx += uint(hashSize)
		case 3:
			// Symref
			var size uint
			idx, size, err = t.getVarInt(idx, b.FullBlockSize)
			if err != nil {
				return nil, fmt.Errorf("getting symref size: %w", err)
			}

			reference.Target = git.ReferenceName(t.src[idx : idx+size]).String()
			reference.IsSymbolic = true
			idx = idx + size
		}

		prefix = refname

		references = append(references, reference)
	}

	return references, nil
}

// parseRefBlock parses a block and if it is a ref block, provides
// all the reference updates.
func (t *reftable) parseRefBlock(headerOffset, blockStart, blockEnd uint) ([]git.Reference, error) {
	currentBS := t.extractBlockLen(blockStart + headerOffset)

	fullBlockSize := t.parseBlockSize()
	if fullBlockSize == 0 {
		fullBlockSize = currentBS
	} else if currentBS < fullBlockSize && currentBS < (blockEnd-blockStart) && t.src[blockStart+currentBS] != 0 {
		fullBlockSize = currentBS
	}

	b := &block{
		BlockStart:    blockStart + headerOffset,
		FullBlockSize: fullBlockSize,
	}

	if err := binary.Read(bytes.NewBuffer(t.src[blockStart+currentBS-2:]), binary.BigEndian, &b.RestartCount); err != nil {
		return nil, fmt.Errorf("reading restart count: %w", err)
	}

	b.RestartStart = blockStart + currentBS - 2 - 3*uint(b.RestartCount)

	return t.getRefsFromBlock(b)
}

// IterateRefs provides all the refs present in a table.
func (t *reftable) IterateRefs() ([]git.Reference, error) {
	if t.footer == nil {
		return nil, fmt.Errorf("table not instantiated")
	}

	offset := uint(0)
	var allRefs []git.Reference

	for offset < t.size {
		headerOffset := uint(0)
		if offset == 0 {
			headerOffset = t.headerSize
		}

		blockStart, blockEnd := t.getBlockRange(offset, t.parseBlockSize())
		if blockStart == 0 && blockEnd == 0 {
			break
		}

		// If we run out of ref blocks, we can stop the iteration.
		if t.src[blockStart+headerOffset] != 'r' {
			return allRefs, nil
		}

		references, err := t.parseRefBlock(headerOffset, blockStart, blockEnd)
		if err != nil {
			return nil, fmt.Errorf("parsing block: %w", err)
		}

		if len(references) == 0 {
			break
		}

		allRefs = append(allRefs, references...)

		offset = blockEnd
	}

	return allRefs, nil
}

// NewReftable instantiates a new reftable from the given reftable content.
func NewReftable(content []byte) (*reftable, error) {
	t := &reftable{src: content}
	block := t.src[0:28]

	var h header
	if err := binary.Read(bytes.NewBuffer(block), binary.BigEndian, &h); err != nil {
		return nil, fmt.Errorf("reading header: %w", err)
	}

	if !bytes.Equal(h.Name[:], []byte("REFT")) {
		return nil, fmt.Errorf("unexpected header name: %s", h.Name)
	}

	if h.Version != 1 && h.Version != 2 {
		return nil, fmt.Errorf("unexpected reftable version: %d", h.Version)
	}

	t.header = &h

	t.footerSize = uint(68)
	t.headerSize = uint(24)
	if h.Version == 2 {
		t.footerSize = 72
		t.headerSize = 28
	}
	t.size = uint(len(t.src)) - t.footerSize

	block = t.src[t.size:len(t.src)]

	var f footer
	if err := binary.Read(bytes.NewBuffer(block), binary.BigEndian, &f.footerBase); err != nil {
		return nil, fmt.Errorf("reading footer: %w", err)
	}

	if f.Name != h.Name ||
		f.Version != h.Version ||
		!bytes.Equal(f.BlockSize[:], h.BlockSize[:]) ||
		f.MinUpdateIndex != h.MinUpdateIndex ||
		f.MaxUpdateIndex != h.MaxUpdateIndex {
		return nil, fmt.Errorf("footer doesn't match header")
	}

	if h.Version == 2 {
		if err := binary.Read(bytes.NewBuffer(block[t.headerSize-4:]), binary.BigEndian, &f.HashID); err != nil {
			return nil, fmt.Errorf("reading hash ID: %w", err)
		}

		if f.HashID != h.HashID {
			return nil, fmt.Errorf("footer doesn't match header")
		}

		if err := binary.Read(bytes.NewBuffer(block[t.headerSize:]), binary.BigEndian, &f.footerEnd); err != nil {
			return nil, fmt.Errorf("reading footer: %w", err)
		}
	} else {
		if err := binary.Read(bytes.NewBuffer(block[t.headerSize:]), binary.BigEndian, &f.footerEnd); err != nil {
			return nil, fmt.Errorf("reading footer: %w", err)
		}
	}

	// TODO: CRC32 validation of the data
	// https://gitlab.com/gitlab-org/git/-/blob/master/reftable/reader.c#L143
	t.footer = &f

	return t, nil
}

// ReadTablesList returns a list of tables in the "tables.list" for the
// reftable backend.
func ReadTablesList(repoPath string) ([]string, error) {
	tablesListPath := filepath.Join(repoPath, "reftable", "tables.list")

	data, err := os.ReadFile(tablesListPath)
	if err != nil {
		return []string{}, fmt.Errorf("reading tables.list: %w", err)
	}

	list := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	for _, line := range list {
		if !NameRegex.MatchString(line) {
			return list, fmt.Errorf("unrecognized reftable name: %s", line)
		}
	}

	return list, nil
}

// Name contains the structured information in the name of a .ref file.
type Name struct {
	// MinUpdateIndex is the minimum update index contained in the file.
	MinUpdateIndex uint64
	// MinUpdateIndex is the maximum update index contained in the file.
	MaxUpdateIndex uint64
}

// ParseName parses the name of a reftable file.
func ParseName(reftableName string) (Name, error) {
	matches := NameRegex.FindAllStringSubmatch(reftableName, -1)
	if len(matches) != 1 || len(matches[0]) != 3 {
		return Name{}, fmt.Errorf("reftable name %q malformed", reftableName)
	}

	minIndex, err := strconv.ParseUint(matches[0][1], 16, 64)
	if err != nil {
		return Name{}, fmt.Errorf("parsing min index: %w", err)
	}

	maxIndex, err := strconv.ParseUint(matches[0][2], 16, 64)
	if err != nil {
		return Name{}, fmt.Errorf("parsing max index: %w", err)
	}

	return Name{
		MinUpdateIndex: minIndex,
		MaxUpdateIndex: maxIndex,
	}, nil
}
