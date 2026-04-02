package linguist

import (
	"fmt"
	"io"

	"github.com/go-enry/go-enry/v2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitattributes"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/gitpipe"
)

const (
	linguistDocumentation = "linguist-documentation"
	linguistDetectable    = "linguist-detectable"
	linguistGenerated     = "linguist-generated"
	linguistVendored      = "linguist-vendored"
	linguistLanguage      = "linguist-language"
)

var linguistAttrs = []string{
	linguistDocumentation,
	linguistDetectable,
	linguistGenerated,
	linguistVendored,
	linguistLanguage,
}

type fileInstance struct {
	filename string
	attrs    gitattributes.Attributes
}

func newFileInstance(filename string, checkAttr *gitattributes.CheckAttrCmd) (*fileInstance, error) {
	attrs, err := checkAttr.Check(filename)
	if err != nil {
		return nil, fmt.Errorf("checking attribute: %w", err)
	}

	return &fileInstance{
		filename: filename,
		attrs:    attrs,
	}, nil
}

func (f fileInstance) isDocumentation() bool {
	if f.attrs.IsUnset(linguistDocumentation) {
		return false
	}
	if f.attrs.IsSet(linguistDocumentation) {
		return true
	}

	return enry.IsDocumentation(f.filename)
}

func (f fileInstance) isVendored() bool {
	if f.attrs.IsUnset(linguistVendored) {
		return false
	}
	if f.attrs.IsSet(linguistVendored) {
		return true
	}

	return enry.IsVendor(f.filename)
}

func (f fileInstance) isGenerated(content []byte) bool {
	if f.attrs.IsUnset(linguistGenerated) {
		return false
	}
	if f.attrs.IsSet(linguistGenerated) {
		return true
	}

	return enry.IsGenerated(f.filename, content)
}

func (f fileInstance) getLanguage(content []byte) string {
	if lang, ok := f.attrs.StateFor(linguistLanguage); ok {
		// The linguist-language attribute may specify a language alias
		// (e.g., "omnetpp-msg" instead of "OMNeT++ MSG"). Resolve it to
		// the canonical name so that downstream consumers like
		// GetLanguageID() and GetLanguageType() work correctly.
		if canonical, ok := enry.GetLanguageByAlias(lang); ok {
			return canonical
		}
		// If the alias is not recognized, return the raw value to
		// preserve user intent and surface misconfiguration.
		return lang
	}

	return enry.GetLanguage(f.filename, content)
}

func (f fileInstance) isIgnoredLanguage(lang string) bool {
	if f.attrs.IsSet(linguistDetectable) {
		return false
	}
	if f.attrs.IsUnset(linguistDetectable) {
		return true
	}

	// Ignore anything that's neither markup nor a programming language,
	// similar to what the linguist gem does:
	// https://github.com/github/linguist/blob/v7.20.0/lib/linguist/blob_helper.rb#L378-L387
	return enry.GetLanguageType(lang) != enry.Programming &&
		enry.GetLanguageType(lang) != enry.Markup
}

// IsExcluded returns whether
func (f fileInstance) IsExcluded() bool {
	return f.isDocumentation() || f.isVendored()
}

// DetermineStats determines the size and language of the given file. The
// language will be an empty string when the stats should be omitted from the
// count.
func (f fileInstance) DetermineStats(object gitpipe.CatfileObjectResult) (string, uint64, error) {
	// Read arbitrary number of bytes considered enough to determine language
	content, err := io.ReadAll(io.LimitReader(object, 1024*16))
	if err != nil {
		return "", 0, fmt.Errorf("determine stats read blob: %w", err)
	}

	if f.isGenerated(content) {
		return "", 0, nil
	}

	lang := f.getLanguage(content)

	if f.isIgnoredLanguage(lang) {
		return "", 0, nil
	}

	return lang, uint64(object.Object.ObjectSize()), nil
}
