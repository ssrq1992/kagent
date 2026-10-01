package database

import (
	"testing"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAXBindingRejectsStaleClaimAndReplacement(t *testing.T) {
	db := setupTestDB(t)
	client := NewClient(db)
	id, operation, executor := uuid.New(), uuid.New(), uuid.New()
	_, err := db.Exec(t.Context(), `INSERT INTO runtime_instance
 (id,kind,user_id,request_id,state,operation,operation_id,executor_id)
 VALUES ($1,'sandbox','user',$2,'RUNTIME_STATE_CREATING','RUNTIME_OPERATION_CREATE',$3,$4)`, id, id.String(), operation, executor)
	require.NoError(t, err)
	binding := AXBinding{Task: &ax.ResourceRef{Atespace: "test", Name: "task", Uid: uuid.NewString()}, Group: &ax.ResourceRef{Atespace: "test", Name: "group", Uid: uuid.NewString()}, PreparedRuntime: &ax.ResourceRef{Atespace: "test", Name: "runtime", Uid: uuid.NewString()}}
	require.ErrorIs(t, client.RecordAXBinding(t.Context(), id.String(), operation, uuid.New(), binding), ErrConflict)
	_, err = client.GetAXBinding(t.Context(), id.String())
	require.ErrorIs(t, err, ErrNotFound)
	require.NoError(t, client.RecordAXBinding(t.Context(), id.String(), operation, executor, binding))
	require.NoError(t, client.RecordAXBinding(t.Context(), id.String(), operation, executor, binding))
	replacement := binding
	replacement.Task = proto.Clone(binding.Task).(*ax.ResourceRef)
	replacement.Task.Uid = uuid.NewString()
	require.ErrorIs(t, client.RecordAXBinding(t.Context(), id.String(), operation, executor, replacement), ErrConflict)
	stored, err := client.GetAXBinding(t.Context(), id.String())
	require.NoError(t, err)
	require.True(t, proto.Equal(stored.Task, binding.Task))
	_, err = db.Exec(t.Context(), `UPDATE runtime_instance SET executor_id=$2 WHERE id=$1`, id, uuid.New())
	require.NoError(t, err)
	require.ErrorIs(t, client.RecordAXBinding(t.Context(), id.String(), operation, executor, binding), ErrConflict)
}
