package refdb

import (
	"gitlab.com/gitlab-org/gitaly/v18/internal/git"
	"gitlab.com/gitlab-org/gitaly/v18/internal/structerr"
)

// ChildReferencesExistError is raised when attempting to create a reference
// in a node which has child references. With the files backend, this corresponds
// to a directory-file conflict where `refs/heads/main/child` exists but we're
// attempting to create a `refs/heads/main`.
type ChildReferencesExistError struct {
	// TargetReference is the name of the reference failed to be created.
	TargetReference git.ReferenceName
}

// Error returns the error message.
func (ChildReferencesExistError) Error() string {
	return "child references exist"
}

// NewChildReferencesExistError returns a new ChildReferencesExistError.
func NewChildReferencesExistError(targetReference git.ReferenceName) error {
	return structerr.NewAborted("%w", ChildReferencesExistError{
		TargetReference: targetReference,
	}).WithMetadata("target_reference", targetReference)
}

// ParentReferenceExistsError is raised when attempting to create a reference
// but a parent reference exists already in the hierarchy. With the files backend,
// this corresponds to a directory-file conflict where `refs/heads/main` exists but
// we're attempting to create a `refs/heads/main/child`.
type ParentReferenceExistsError struct {
	// ExistingReference is the name of the reference that already exists.
	ExistingReference git.ReferenceName
	// TargetReference is the name of the reference that failed to be created.
	TargetReference git.ReferenceName
}

// Error returns the error message.
func (ParentReferenceExistsError) Error() string {
	return "parent reference exists"
}

// NewParentReferenceExistsError returns a new ParentReferenceExistsError.
func NewParentReferenceExistsError(existingReference, targetReference git.ReferenceName) error {
	return structerr.NewAborted("%w", ParentReferenceExistsError{
		ExistingReference: existingReference,
		TargetReference:   targetReference,
	}).WithMetadataItems(
		structerr.MetadataItem{Key: "existing_reference", Value: existingReference},
		structerr.MetadataItem{Key: "target_reference", Value: targetReference},
	)
}

// UnexpectedOldValueError is returned when the reference's old value does not match
// the expected.
type UnexpectedOldValueError struct {
	// TargetReference is the name of the reference that failed to be created, deleted
	// or updated.
	TargetReference git.ReferenceName
	// ExpectedValue is the expected value of the reference prior to the update.
	ExpectedValue string
	// ActualValue is the actual value of the reference prior to the update.
	ActualValue string
}

// Error returns the error message.
func (err UnexpectedOldValueError) Error() string {
	return "unexpected old value"
}

// NewUnexpectedOldValueError returns a new UnexpectedOldValueError.
func NewUnexpectedOldValueError(targetReference git.ReferenceName, expectedValue, actualValue string) error {
	return structerr.NewAborted("%w", UnexpectedOldValueError{
		TargetReference: targetReference,
		ExpectedValue:   expectedValue,
		ActualValue:     actualValue,
	}).WithMetadataItems(
		structerr.MetadataItem{Key: "target_reference", Value: targetReference},
		structerr.MetadataItem{Key: "expected_value", Value: expectedValue},
		structerr.MetadataItem{Key: "actual_value", Value: actualValue},
	)
}
