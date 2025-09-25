package git

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash"

	"gitlab.com/gitlab-org/gitaly/v18/proto/go/gitalypb"
)

var (
	// ObjectHashSHA1 is the implementation of an object ID via SHA1.
	ObjectHashSHA1 = ObjectHash{
		Hash:         sha1.New,
		EmptyTreeOID: ObjectID("4b825dc642cb6eb9a060e54bf8d69288fbee4904"),
		ZeroOID:      ObjectID("0000000000000000000000000000000000000000"),
		Format:       "sha1",
		ProtoFormat:  gitalypb.ObjectFormat_OBJECT_FORMAT_SHA1,
	}

	// ObjectHashSHA256 is the implementation of an object ID via SHA256.
	ObjectHashSHA256 = ObjectHash{
		Hash:         sha256.New,
		EmptyTreeOID: ObjectID("6ef19b41225c5369f1c104d45d8d85efa9b057b53b14b4b9b939dd74decc5321"),
		ZeroOID:      ObjectID("0000000000000000000000000000000000000000000000000000000000000000"),
		Format:       "sha256",
		ProtoFormat:  gitalypb.ObjectFormat_OBJECT_FORMAT_SHA256,
	}
)

// InvalidObjectIDLengthError is returned when an object ID's string
// representation is not the required length.
type InvalidObjectIDLengthError struct {
	OID           string
	CorrectLength int
	Length        int
}

func (e InvalidObjectIDLengthError) Error() string {
	return fmt.Sprintf("invalid object ID: %q, expected length %v, got %v", e.OID, e.CorrectLength, e.Length)
}

// InvalidObjectIDCharError is returned when an object ID's string
// representation contains an invalid hexadecimal digit.
type InvalidObjectIDCharError struct {
	OID     string
	BadChar rune
}

func (e InvalidObjectIDCharError) Error() string {
	return fmt.Sprintf("invalid object ID: %q, invalid character %q", e.OID, e.BadChar)
}

// ObjectHash is a hash-function specific implementation of an object ID.
type ObjectHash struct {
	// Hash is the hashing function used to hash objects.
	Hash func() hash.Hash
	// EmptyTreeOID is the object ID of the tree object that has no directory entries.
	EmptyTreeOID ObjectID
	// ZeroOID is the special value that Git uses to signal a ref or object does not exist
	ZeroOID ObjectID
	// Format is the name of the object hash.
	Format string
	// ProtoFormat is the Protobuf representation of the object format.
	ProtoFormat gitalypb.ObjectFormat
}

// ObjectHashByFormat looks up the ObjectHash by its format name.
func ObjectHashByFormat(format string) (ObjectHash, error) {
	switch format {
	case ObjectHashSHA1.Format:
		return ObjectHashSHA1, nil
	case ObjectHashSHA256.Format:
		return ObjectHashSHA256, nil
	default:
		return ObjectHash{}, fmt.Errorf("unknown object format: %q", format)
	}
}

// ObjectHashByProto looks up the ObjectHash by its Protobuf representation `gitalypb.ObjectFormat`.
// Returns an error in case the object format is not known.
func ObjectHashByProto(format gitalypb.ObjectFormat) (ObjectHash, error) {
	switch format {
	case gitalypb.ObjectFormat_OBJECT_FORMAT_UNSPECIFIED, gitalypb.ObjectFormat_OBJECT_FORMAT_SHA1:
		return ObjectHashSHA1, nil
	case gitalypb.ObjectFormat_OBJECT_FORMAT_SHA256:
		return ObjectHashSHA256, nil
	default:
		return ObjectHash{}, fmt.Errorf("unknown object format: %q", format)
	}
}

// EncodedLen returns the length of the hex-encoded string of a full object ID.
func (h ObjectHash) EncodedLen() int {
	return hex.EncodedLen(h.Hash().Size())
}

// FromHex constructs a new ObjectID from the given hex representation of the object ID. Returns
// ErrInvalidObjectID if the given object ID is not valid.
func (h ObjectHash) FromHex(hex string) (ObjectID, error) {
	if err := h.ValidateHex(hex); err != nil {
		return "", err
	}

	return ObjectID(hex), nil
}

// HashData hashes the given bytes to an object ID.
func (h ObjectHash) HashData(bytes []byte) ObjectID {
	hash := h.Hash()
	// hash.Write never returns an error, so we are free to ignore its error here.
	_, _ = hash.Write(bytes)
	return ObjectID(hex.EncodeToString(hash.Sum(nil)))
}

// ValidateHex checks if `hex` is a syntactically correct object ID for the given hash. Abbreviated
// object IDs are not deemed to be valid. Returns an `ErrInvalidObjectID` if the `hex` is not valid.
func (h ObjectHash) ValidateHex(hex string) error {
	if len(hex) != h.EncodedLen() {
		return InvalidObjectIDLengthError{OID: hex, CorrectLength: h.EncodedLen(), Length: len(hex)}
	}

	for _, c := range hex {
		switch {
		case '0' <= c && c <= '9':
		case 'a' <= c && c <= 'f':
		default:
			return InvalidObjectIDCharError{OID: hex, BadChar: c}
		}
	}

	return nil
}

// IsZeroOID checks whether the given object ID is the all-zeroes object ID for the given hash.
func (h ObjectHash) IsZeroOID(oid ObjectID) bool {
	return string(oid) == string(h.ZeroOID)
}

// ObjectID represents an object ID.
type ObjectID string

// String returns the hex representation of the ObjectID.
func (oid ObjectID) String() string {
	return string(oid)
}

// Bytes returns the byte representation of the ObjectID.
func (oid ObjectID) Bytes() ([]byte, error) {
	decoded, err := hex.DecodeString(string(oid))
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

// Revision returns a revision of the ObjectID. This directly returns the hex
// representation as every object ID is a valid revision.
func (oid ObjectID) Revision() Revision {
	return Revision(oid.String())
}
