package function

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
)

var (
	// GcpProjectID is the ID of the GCP project.
	GcpProjectID = ""

	// GcpZone is the ID of the GCP zone in which resources are.
	GcpZone = ""

	// cutOffDate is the date before which resources are deleted
	// Currently this is set to a week ago (ie: minus 7 days)
	cutOffDate = time.Now().AddDate(0, 0, -14)

	// labelFilter is the filter used to find resources to delete. This label
	// is set by the Terraform project in /_support/benchmark/terraform
	labelFilter = fmt.Sprintf("labels.%s=%s", "benchmark", "gitaly")
)

// Response is the objects that is returned and logged
// at the end of the function. It contains all deleted
// resources and all errors that occurred during an
// execution. This helps a lot for debugging.
type Response struct {
	DeletedResources []Resource `json:"deletedResources"`
	Errors           []error    `json:"errors"`
}

// NewResponse creates a new Response instance
func NewResponse() Response {
	return Response{
		DeletedResources: []Resource{},
		Errors:           []error{},
	}
}

// ResourceCleaner is the function type that each resource
// must implement in order for the resources to be deleted
type ResourceCleaner func(ctx context.Context) Response

func init() {
	GcpProjectID = os.Getenv("GCP_PROJECT_ID")
	GcpZone = os.Getenv("GCP_ZONE")
	functions.HTTP("Run", run)
}

// run is the entry point of the function
func run(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()

	// Resources are deleted in the same order
	//as defined here
	funcs := []ResourceCleaner{
		Instances,
		Disks,
	}

	response := Response{
		DeletedResources: make([]Resource, 0),
		Errors:           make([]error, 0),
	}

	for _, f := range funcs {
		res := f(ctx)

		// Collect the response of each resource into a global
		// response object.
		response.Errors = append(response.Errors, res.Errors...)
		response.DeletedResources = append(response.DeletedResources, res.DeletedResources...)
	}

	// Printing the response to help debugging later
	bytes, err := json.Marshal(response)
	if err != nil {
		fmt.Printf("Error marshalling response: %v; Raw response was: %v", err, response)
	} else {
		fmt.Println(string(bytes))
	}
	return
}
