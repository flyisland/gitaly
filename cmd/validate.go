package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/pelletier/go-toml/v2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/errors/cfgerror"
)

// Validate invokes validator and processes the result. If there are any errors returned
// by validator they will be written in JSON format into the outWriter.
// If there are any runtime errors they will be written into errWriter.
// It returns true if no errors occurred, otherwise it returns false.
func Validate(validator interface{ ValidateV2() error }, outWriter io.Writer, errWriter io.Writer) bool {
	out := validationOutput{}
	for _, err := range cfgerror.New().Append(validator.ValidateV2()) {
		out.Errors = append(out.Errors, validationOutputError{
			Key:     err.Key,
			Message: err.Cause.Error(),
		})
	}

	if len(out.Errors) > 0 {
		jsonEncoded(outWriter, errWriter, "  ", out)
		return false
	}

	return true
}

// WriteTomlReadError writes err into outWriter in case it is a deserialization error that can be
// caused by the wrong values used in the configuration file or invalid file format, returns true.
// Otherwise, err is written into the errWriter, and it returns false.
func WriteTomlReadError(err error, outWriter io.Writer, errWriter io.Writer) bool {
	terr := &toml.DecodeError{}
	if errors.As(err, &terr) {
		row, column := terr.Position()
		jsonEncoded(outWriter, errWriter, "  ", validationOutput{Errors: []validationOutputError{{
			Key:     terr.Key(),
			Message: fmt.Sprintf("line %d column %d: %v", row, column, terr.Error()),
		}}})
		return true
	}

	_, _ = io.WriteString(errWriter, fmt.Sprintf("processing input data: %v", err))
	return false
}

type validationOutputError struct {
	Key     []string `json:"key,omitempty"`
	Message string   `json:"message,omitempty"`
}

type validationOutput struct {
	Errors []validationOutputError `json:"errors,omitempty"`
}

func jsonEncoded(outStream io.Writer, errStream io.Writer, indent string, val any) {
	encoder := json.NewEncoder(outStream)
	encoder.SetIndent("", indent)
	if err := encoder.Encode(val); err != nil {
		fmt.Fprintf(errStream, "writing results: %v\n", err)
	}
}
