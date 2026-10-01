package axruntime

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
)

// Reference contains only public AX identities. BoundaryRef is opaque and must
// never be interpreted as a provider storage URI or a network authority.
type Reference struct {
	Version     int             `json:"version"`
	Task        *ax.ResourceRef `json:"task"`
	Runtime     *ax.ResourceRef `json:"preparedRuntime"`
	Group       *ax.ResourceRef `json:"group"`
	BoundaryRef string          `json:"boundaryRef,omitempty"`
	Checkpoint  *ax.ResourceRef `json:"checkpoint,omitempty"`
}

func ReferenceFromTask(task *ax.Task) Reference {
	return Reference{Version: 1, Task: ax.Ref(task.GetMetadata()), Runtime: task.GetSpec().GetPreparedRuntimeRef(), Group: task.GetSpec().GetGroupRef(), BoundaryRef: task.GetStatus().GetRuntimeStatus().GetBoundaryRef()}
}
func (r Reference) Validate() error {
	if r.Version != 1 {
		return fmt.Errorf("unsupported AX reference version")
	}
	for _, ref := range []*ax.ResourceRef{r.Task, r.Runtime, r.Group} {
		if err := ax.ValidateRef(ref, true); err != nil {
			return err
		}
		if ref.Atespace != r.Task.Atespace {
			return fmt.Errorf("cross-namespace AX reference")
		}
	}
	if r.Checkpoint != nil {
		if err := ax.ValidateRef(r.Checkpoint, true); err != nil {
			return err
		}
		if r.Checkpoint.Atespace != r.Task.Atespace || r.BoundaryRef == "" {
			return fmt.Errorf("incomplete AX checkpoint reference")
		}
	}
	return nil
}
func (r Reference) Encode() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(r)
	return string(raw), err
}
func DecodeReference(raw string) (Reference, error) {
	var ref Reference
	decoder := json.NewDecoder(bytes.NewBufferString(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ref); err != nil {
		return ref, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return ref, fmt.Errorf("trailing data in AX reference")
	}
	return ref, ref.Validate()
}
