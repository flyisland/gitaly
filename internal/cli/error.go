package cli

import "fmt"

// RequiredFlagError type is needed to produce the same error message as one from the
// github.com/urfave/cli/v3 package. Unfortunately the errRequiredFlags type is not
// exportable, and we can't utilise it.
type RequiredFlagError string

func (rf RequiredFlagError) Error() string {
	return fmt.Sprintf("Required flag %q not set", string(rf))
}
