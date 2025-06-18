package reftable

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// version represents a reftable version.
type version uint8

// HeaderSize returns the size of the header for this reftable version.
func (v version) HeaderSize() int {
	switch v {
	case 1:
		// The Size is documented at https://git-scm.com/docs/reftable#_header_version_1
		return 24
	case 2:
		// The size  is documented at https://git-scm.com/docs/reftable#_header_version_2
		return 28
	default:
		panic(fmt.Errorf("unsupported version: %d", v))
	}
}

// FooterSize returns the size of the footer for this reftable version.
func (v version) FooterSize() int {
	// The footer sizes are documented at https://git-scm.com/docs/reftable#_footer.
	switch v {
	case 1:
		return 68
	case 2:
		return 72
	default:
		panic(fmt.Errorf("unsupported version: %d", v))
	}
}

// headerV1 represents the header in reftable version 1.
type headerV1 struct {
	Magic          [4]byte
	Version        version
	BlockSize      [3]byte
	MinUpdateIndex uint64
	MaxUpdateIndex uint64
}

// header represents the header of a reftable.
type header struct {
	headerV1
	// HashID is only present if version is 2
	HashID [4]byte
}

// parseHeader parses the header of a reftable. reader should be at the beginning
// of the header.
func parseHeader(reader io.Reader, hdr *header) error {
	if err := binary.Read(reader, binary.BigEndian, &hdr.headerV1); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}

	if hdr.Magic != [...]byte{'R', 'E', 'F', 'T'} {
		return fmt.Errorf("unexpected magic bytes: %q", hdr.Magic)
	}

	if !(hdr.Version == 1 || hdr.Version == 2) {
		return fmt.Errorf("unsupported version: %d", hdr.Version)
	}

	if hdr.Version == 2 {
		if err := binary.Read(reader, binary.BigEndian, &hdr.HashID); err != nil {
			return fmt.Errorf("read hash id: %w", err)
		}

		if !(hdr.HashID == [...]byte{'s', 'h', 'a', '1'} || hdr.HashID == [...]byte{'s', '2', '5', '6'}) {
			return fmt.Errorf("unsupported hash id: %q", hdr.HashID)
		}
	}

	return nil
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
	header
	footerEnd
}

// parseFooter parses the footer of a reftable. reader should be at the beginning
// of the footer.
func parseFooter(reader io.Reader, f *footer) error {
	if err := parseHeader(reader, &f.header); err != nil {
		return fmt.Errorf("parse header: %w", err)
	}

	if err := binary.Read(reader, binary.BigEndian, &f.footerEnd); err != nil {
		return fmt.Errorf("parse remainder: %w", err)
	}

	return nil
}

type block struct {
	BlockStart    uint
	FullBlockSize uint
	HeaderOffset  uint
	RestartCount  uint16
	RestartStart  uint
}

type reftable struct {
	blockSize *uint
	size      uint
	src       []byte
	footer    *footer
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
		blockSize := uint(big.NewInt(0).SetBytes(t.footer.BlockSize[:]).Uint64())
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
			headerOffset = uint(t.footer.Version.HeaderSize())
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

	var h header
	if err := parseHeader(bytes.NewReader(content), &h); err != nil {
		return nil, fmt.Errorf("parse header: %w", err)
	}

	t.size = uint(len(t.src) - h.Version.FooterSize())

	var f footer
	if err := parseFooter(bytes.NewReader(t.src[t.size:]), &f); err != nil {
		return nil, fmt.Errorf("parse footer: %w", err)
	}

	if h != f.header {
		return nil, fmt.Errorf("footer doesn't match header")
	}

	// TODO: CRC32 validation of the data
	// https://gitlab.com/gitlab-org/git/-/blob/master/reftable/reader.c#L143
	t.footer = &f

	return t, nil
}

// ReadTablesList returns a list of tables in the "tables.list" for the
// reftable backend.
func ReadTablesList(repoPath string) ([]Name, error) {
	tablesListPath := filepath.Join(repoPath, "reftable", "tables.list")

	data, err := os.ReadFile(tablesListPath)
	if err != nil {
		return nil, fmt.Errorf("reading tables.list: %w", err)
	}

	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	names := make([]Name, len(lines))
	for i, line := range lines {
		if names[i], err = ParseName(line); err != nil {
			return nil, fmt.Errorf("parse name: %w", err)
		}
	}

	return names, nil
}

// Name contains the structured information in the name of a .ref file.
type Name struct {
	// MinUpdateIndex is the minimum update index contained in the file.
	MinUpdateIndex uint64
	// MinUpdateIndex is the maximum update index contained in the file.
	MaxUpdateIndex uint64
	// Suffix is the random suffix in the table's name.
	Suffix string
}

// String returns the string representation of the reftable name.
func (n Name) String() string {
	return fmt.Sprintf("0x%012x-0x%012x-%s.ref", n.MinUpdateIndex, n.MaxUpdateIndex, n.Suffix)
}

// nameRegex is a regex for matching reftable names
// e.g. 0x000000000001-0x00000000000a-b54f3b59.ref would result in the following submatches:
//   - 000000000001 (UpdateIndexMin)
//   - 00000000000a (UpdateIndexMax)
//   - b54f3b59     (Suffix)
//
// See the reftable documentation at https://www.git-scm.com/docs/reftable#_layout for more
// information.
var nameRegex = regexp.MustCompile("^0x([[:xdigit:]]{12})-0x([[:xdigit:]]{12})-([0-9a-zA-Z]{8}).ref$")

// ParseName parses the name of a reftable file.
func ParseName(reftableName string) (Name, error) {
	matches := nameRegex.FindStringSubmatch(reftableName)
	if len(matches) == 0 {
		return Name{}, fmt.Errorf("reftable name %q malformed", reftableName)
	}

	minIndex, err := strconv.ParseUint(matches[1], 16, 64)
	if err != nil {
		return Name{}, fmt.Errorf("parsing min index: %w", err)
	}

	maxIndex, err := strconv.ParseUint(matches[2], 16, 64)
	if err != nil {
		return Name{}, fmt.Errorf("parsing max index: %w", err)
	}

	return Name{
		MinUpdateIndex: minIndex,
		MaxUpdateIndex: maxIndex,
		Suffix:         matches[3],
	}, nil
}
