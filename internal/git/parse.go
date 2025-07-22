package git

import (
	"strconv"
	"strings"
	"time"

	"gitlab.com/gitlab-org/gitaly/v16/proto/go/gitalypb"
)

const maxUnixCommitDate = 1 << 53

// ParseDateSeconds turns a seconds field in a Git commit into a unix timestamp
func ParseDateSeconds(seconds string) int64 {
	sec, err := strconv.ParseInt(seconds, 10, 64)
	if err != nil || sec > maxUnixCommitDate || sec < 0 {
		sec = fallbackTimeValue.Unix()
	}

	return sec
}

// fallbackTimeValue is the value returned in case there is a parse error. It's the maximum
// time value possible in golang. See
// https://gitlab.com/gitlab-org/gitaly/issues/556#note_40289573
var fallbackTimeValue = time.Unix(1<<63-62135596801, 999999999)

// DetectSignatureType detects the signature type of a git commit signature
func DetectSignatureType(line string) gitalypb.SignatureType {
	switch strings.TrimSuffix(line, "\n") {
	case "-----BEGIN SIGNED MESSAGE-----":
		return gitalypb.SignatureType_X509
	case "-----BEGIN PGP MESSAGE-----":
		return gitalypb.SignatureType_PGP
	case "-----BEGIN PGP SIGNATURE-----":
		return gitalypb.SignatureType_PGP
	case "-----BEGIN SSH SIGNATURE-----":
		return gitalypb.SignatureType_SSH
	default:
		return gitalypb.SignatureType_NONE
	}
}
