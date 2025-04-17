package diff

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"

	"gitlab.com/gitlab-org/gitaly/v16/internal/git"
)

// PatchParser defines the parser state required for parsing diffs. Patch output from a git diff
// family command is parsed line by line. Patches are delimited by git diff headers.
type PatchParser struct {
	// reader contains the patch output for a diff command.
	reader *bufio.Reader
	// rawInfo specifies the list of patches in the expected parsing order.
	rawInfo []Raw
	// patchLimit specifies the max byte size of an individual patch. If exceeded, the patch data
	// is pruned.
	patchLimit int32
	// objectHash specifies the format of the object IDs.
	objectHash git.ObjectHash
	// diff contains the last parsed diff and is reused for every patch.
	diff Diff
	// err records any error encountered during parsing operations.
	err error
	// nextSrcPath stores the path of the next patch to be parsed. A patch only gets read once the
	// parser is processing the corresponding raw entry. Raw output that does not have matching
	// patch output is treated as an empty patch.
	nextSrcPath []byte
}

// NewPatchParser initializes and returns a new diff parser.
func NewPatchParser(reader io.Reader, rawInfo []Raw, patchLimit int32, objectHash git.ObjectHash) *PatchParser {
	return &PatchParser{
		reader:     bufio.NewReader(reader),
		rawInfo:    rawInfo,
		patchLimit: patchLimit,
		objectHash: objectHash,
	}
}

// Parse parses a single diff. It returns true if successful, false if it finished parsing all
// diffs or when it encounters an error, in which case use Parser.Err() to get the error.
func (p *PatchParser) Parse() bool {
	if len(p.rawInfo) == 0 {
		// If there is nothing left to parse, there should also be no remaining patch output to
		// consume. Discard any remaining output just to be sure.
		_, _ = io.Copy(io.Discard, p.reader)
		return false
	}

	raw := p.rawInfo[0]
	p.rawInfo = p.rawInfo[1:]

	// Type changes are reflected in Git diff output as a removal followed by an addition. The raw
	// diff info is used to define the expected order of parsed diff output and provide mode and
	// OID info. This also allows responses to be sent for diffs with no patch output. Therefore,
	// the type change status must be handled specially to maintain the correct expected order.
	if raw.Status == 'T' {
		addedRaw := raw
		addedRaw.SrcMode = 0
		addedRaw.SrcOID = p.objectHash.ZeroOID.String()
		addedRaw.Status = 'A'

		raw.DstMode = 0
		raw.DstOID = p.objectHash.ZeroOID.String()
		raw.Status = 'R'

		p.rawInfo = append([]Raw{addedRaw}, p.rawInfo...)
	}

	p.diff.Reset()
	p.diff.FromID = raw.SrcOID
	p.diff.ToID = raw.DstOID
	p.diff.FromPath = raw.SrcPath
	p.diff.ToPath = raw.DstPath

	if p.nextSrcPath == nil {
		srcPath, err := readDiffHeaderFromPath(p.reader)
		if err != nil && !errors.Is(err, io.EOF) {
			p.err = fmt.Errorf("read diff header: %w", err)
			return false
		}
		p.nextSrcPath = srcPath
	}

	if bytes.Equal(p.nextSrcPath, raw.SrcPath) {
		p.nextSrcPath = nil

		if err := readNextDiff(p.reader, &p.diff, false); err != nil {
			p.err = fmt.Errorf("read diff content: %w", err)
			return false
		}

		p.diff.PatchSize = int32(len(p.diff.Patch))
		if p.patchLimit != 0 && p.diff.PatchSize > p.patchLimit {
			p.diff.TooLarge = true
			p.diff.ClearPatch()
		}
	}

	return true
}

// Diff returns a successfully parsed diff. It should be called only when Parser.Parse() returns
// true. The return value is valid only until the next call to Parser.Parse().
func (p *PatchParser) Diff() *Diff {
	return &p.diff
}

// Err returns the error encountered (if any) when parsing the diff stream. It should be called
// only when Parser.Parse() returns false.
func (p *PatchParser) Err() error {
	return p.err
}

// Raw defines raw formatted diff output. See https://git-scm.com/docs/diff-format#_raw_output_format.
type Raw struct {
	SrcMode int32
	DstMode int32
	SrcOID  string
	DstOID  string
	Status  byte
	Score   int32
	SrcPath []byte
	DstPath []byte
}

// ToBytes output a raw diff line in the nul-delimited format.
func (r Raw) ToBytes() []byte {
	var raw bytes.Buffer

	raw.WriteString(fmt.Sprintf(":%06o %06o %s %s %c",
		r.SrcMode, r.DstMode, r.SrcOID, r.DstOID, r.Status))

	if r.Status == 'R' || r.Status == 'C' {
		raw.WriteString(fmt.Sprintf("%03d", r.Score))
	}

	raw.WriteByte(0x00)
	raw.Write(r.SrcPath)

	if len(r.DstPath) > 0 {
		raw.WriteByte(0x00)
		raw.Write(r.DstPath)
	}

	raw.WriteByte(0x00)

	return raw.Bytes()
}
