package trace2hooks

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gitlab.com/gitlab-org/gitaly/v18/internal/git/trace2"
	"gitlab.com/gitlab-org/gitaly/v18/internal/log"
	"gitlab.com/gitlab-org/gitaly/v18/internal/testhelper"
)

func TestPackObjectsMetrics_Handle(t *testing.T) {
	t.Parallel()

	// Pin a timestamp for trace tree generation below. This way enables asserting the time
	// frames of spans correctly.
	current, err := time.Parse("2006-01-02T15:04:05Z", "2023-01-01T00:00:00Z")
	require.NoError(t, err)

	for _, tc := range []struct {
		desc            string
		inputTrace      *trace2.Trace
		expectedMetrics string
		expectedFields  log.Fields
	}{
		{
			desc:            "empty trace",
			inputTrace:      nil,
			expectedFields:  log.Fields{},
			expectedMetrics: ``,
		},
		{
			desc: "receives trace consisting of root only",
			inputTrace: &trace2.Trace{
				Thread:     "main",
				Name:       "root",
				StartTime:  current,
				FinishTime: time.Time{},
			},
			expectedFields:  log.Fields{},
			expectedMetrics: ``,
		},
		{
			desc: "receives a complete trace",
			inputTrace: connectChildren(&trace2.Trace{
				Thread:     "main",
				Name:       "root",
				StartTime:  current,
				FinishTime: current.Add(9 * time.Second),
				Children: []*trace2.Trace{
					{
						Thread:     "main",
						Name:       "version",
						StartTime:  current,
						FinishTime: current.Add(1 * time.Second),
					},
					{
						Thread:     "main",
						Name:       "start",
						StartTime:  current.Add(1 * time.Second),
						FinishTime: current.Add(2 * time.Second),
						Metadata:   map[string]string{"argv": "git pack-objects toon --compression=0"},
					},
					{
						Thread:     "main",
						Name:       "def_repo",
						StartTime:  current.Add(2 * time.Second),
						FinishTime: current.Add(3 * time.Second),
					},
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:enumerate-objects",
						StartTime:  current.Add(3 * time.Second),
						FinishTime: current.Add(5 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:  "main",
								Name:    "data:pack-objects:stdin_packs_hints",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "999",
								},
								StartTime:  current.Add(3 * time.Second),
								FinishTime: current.Add(4 * time.Second),
							},
							{
								Thread:  "main",
								Name:    "data:pack-objects:stdin_packs_found",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "998",
								},
								StartTime:  current.Add(4 * time.Second),
								FinishTime: current.Add(5 * time.Second),
							},
						},
					}),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:prepare-pack",
						StartTime:  current.Add(5 * time.Second),
						FinishTime: current.Add(7 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:     "main",
								Name:       "progress:Counting objects",
								ChildID:    "0",
								StartTime:  current.Add(5 * time.Second),
								FinishTime: current.Add(6 * time.Second),
							},
							{
								Thread:  "main",
								Name:    "data:pack-objects:loosen_unused_packed_objects/loosened",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "1234",
								},
								StartTime:  current.Add(6 * time.Second),
								FinishTime: current.Add(7 * time.Second),
							},
						},
					},
					),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:write-pack-file",
						StartTime:  current.Add(7 * time.Second),
						FinishTime: current.Add(9 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:     "main",
								Name:       "progress:Writing objects",
								ChildID:    "0",
								StartTime:  current.Add(7 * time.Second),
								FinishTime: current.Add(8 * time.Second),
							},
							{
								Thread:  "main",
								Name:    "data:pack-objects:write_pack_file/wrote",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "99",
								},
								StartTime:  current.Add(8 * time.Second),
								FinishTime: current.Add(9 * time.Second),
							},
						},
					},
					),
				},
			}),
			expectedFields: log.Fields{
				"pack_objects.written_object_count":           99,
				"pack_objects.loosened_unused_packed_objects": 1234,
				"pack_objects.stdin_packs_hints":              999,
				"pack_objects.stdin_packs_found":              998,
				"pack_objects.enumerate_objects_ms":           2000,
				"pack_objects.prepare_pack_ms":                2000,
				"pack_objects.write_pack_file_ms":             2000,
			},
			expectedMetrics: `# HELP gitaly_pack_objects_stages_seconds Time of pack-objects command on different stage
# TYPE gitaly_pack_objects_stages_seconds histogram
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="2.5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="10"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="+Inf"} 1
gitaly_pack_objects_stages_seconds_sum{stage="enumerate-objects"} 2
gitaly_pack_objects_stages_seconds_count{stage="enumerate-objects"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="2.5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="10"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="+Inf"} 1
gitaly_pack_objects_stages_seconds_sum{stage="prepare-pack"} 2
gitaly_pack_objects_stages_seconds_count{stage="prepare-pack"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="2.5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="10"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="+Inf"} 1
gitaly_pack_objects_stages_seconds_sum{stage="write-pack-file"} 2
gitaly_pack_objects_stages_seconds_count{stage="write-pack-file"} 1

`,
		},
		{
			desc: "tree contains multiple relative fields",
			inputTrace: connectChildren(&trace2.Trace{
				Thread:     "main",
				Name:       "root",
				StartTime:  current,
				FinishTime: current.Add(9 * time.Second),
				Children: []*trace2.Trace{
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:enumerate-objects",
						StartTime:  current.Add(3 * time.Second),
						FinishTime: current.Add(5 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:  "main",
								Name:    "data:pack-objects:stdin_packs_hints",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "999",
								},
								StartTime:  current.Add(3 * time.Second),
								FinishTime: current.Add(4 * time.Second),
							},
							{
								Thread:  "main",
								Name:    "data:pack-objects:stdin_packs_found",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "998",
								},
								StartTime:  current.Add(4 * time.Second),
								FinishTime: current.Add(5 * time.Second),
							},
						},
					}),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:prepare-pack",
						StartTime:  current,
						FinishTime: current.Add(5 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:  "main",
								Name:    "data:pack-objects:loosen_unused_packed_objects/loosened",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "1234",
								},
							},
							{
								Thread:  "main",
								Name:    "data:pack-objects:loosen_unused_packed_objects/loosened",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "3456",
								},
							},
						},
					},
					),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:prepare-pack",
						StartTime:  current.Add(5 * time.Second),
						FinishTime: current.Add(7 * time.Second),
						Children:   []*trace2.Trace{},
					},
					),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:write-pack-file",
						StartTime:  current.Add(7 * time.Second),
						FinishTime: current.Add(8 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:  "main",
								Name:    "data:pack-objects:write_pack_file/wrote",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "99",
								},
							},
						},
					},
					),
					connectChildren(&trace2.Trace{
						Thread:     "main",
						Name:       "pack-objects:write-pack-file",
						StartTime:  current.Add(8 * time.Second),
						FinishTime: current.Add(9 * time.Second),
						Children: []*trace2.Trace{
							{
								Thread:  "main",
								Name:    "data:pack-objects:write_pack_file/wrote",
								ChildID: "0",
								Metadata: map[string]string{
									"data": "2",
								},
							},
						},
					},
					),
				},
			}),
			expectedFields: log.Fields{
				"pack_objects.written_object_count":           101,
				"pack_objects.loosened_unused_packed_objects": 4690,
				"pack_objects.stdin_packs_hints":              999,
				"pack_objects.stdin_packs_found":              998,
				"pack_objects.enumerate_objects_ms":           2000,
				"pack_objects.prepare_pack_ms":                7000,
				"pack_objects.write_pack_file_ms":             2000,
			},
			expectedMetrics: `# HELP gitaly_pack_objects_stages_seconds Time of pack-objects command on different stage
# TYPE gitaly_pack_objects_stages_seconds histogram
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="2.5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="10"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="enumerate-objects",le="+Inf"} 1
gitaly_pack_objects_stages_seconds_sum{stage="enumerate-objects"} 2
gitaly_pack_objects_stages_seconds_count{stage="enumerate-objects"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="2.5"} 1
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="5"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="10"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="prepare-pack",le="+Inf"} 2
gitaly_pack_objects_stages_seconds_sum{stage="prepare-pack"} 7
gitaly_pack_objects_stages_seconds_count{stage="prepare-pack"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.005"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.01"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.025"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.05"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.1"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.25"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="0.5"} 0
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="1"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="2.5"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="5"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="10"} 2
gitaly_pack_objects_stages_seconds_bucket{stage="write-pack-file",le="+Inf"} 2
gitaly_pack_objects_stages_seconds_sum{stage="write-pack-file"} 2
gitaly_pack_objects_stages_seconds_count{stage="write-pack-file"} 2

`,
		},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			ctx := log.InitContextCustomFields(testhelper.Context(t))

			exporter := NewPackObjectsMetrics()

			err := exporter.Handle(ctx, tc.inputTrace)
			require.NoError(t, err)

			testhelper.RequirePromMetrics(t, exporter, tc.expectedMetrics)

			customFields := log.CustomFieldsFromContext(ctx)
			require.NotNil(t, customFields)
			require.Equal(t, tc.expectedFields, customFields.Fields())
		})
	}
}
