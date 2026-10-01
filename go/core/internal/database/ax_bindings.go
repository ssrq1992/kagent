package database

import (
	"context"
	"fmt"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/proto"
)

// AXBinding pins the public AX identities independently of business lifecycle.
// It is written immediately after a known CreateTask result, before starting a
// Sandbox, so a later failed resume cannot lose the new Task UID.
type AXBinding struct {
	Task            *ax.ResourceRef
	Group           *ax.ResourceRef
	PreparedRuntime *ax.ResourceRef
}
type axBindingRow struct {
	Atespace            string
	TaskName            string
	TaskUID             string
	GroupName           string
	GroupUID            string
	PreparedRuntimeName string
	PreparedRuntimeUID  string
}

func (r axBindingRow) binding() AXBinding {
	return AXBinding{Task: &ax.ResourceRef{Atespace: r.Atespace, Name: r.TaskName, Uid: r.TaskUID}, Group: &ax.ResourceRef{Atespace: r.Atespace, Name: r.GroupName, Uid: r.GroupUID}, PreparedRuntime: &ax.ResourceRef{Atespace: r.Atespace, Name: r.PreparedRuntimeName, Uid: r.PreparedRuntimeUID}}
}
func readAXBinding(ctx context.Context, db dbExecutor, id string) (AXBinding, error) {
	row, err := queryOne(ctx, db, `SELECT atespace, task_name, task_uid, group_name, group_uid,
 prepared_runtime_name, prepared_runtime_uid FROM ax_runtime_binding WHERE runtime_id = $1`, pgx.RowToStructByName[axBindingRow], id)
	if err != nil {
		return AXBinding{}, notFoundOr(err)
	}
	return row.binding(), nil
}
func (c *Client) GetAXBinding(ctx context.Context, id string) (AXBinding, error) {
	return readAXBinding(ctx, c.db, id)
}

// RecordAXBinding only accepts the still-current business executor. Retrying a
// known result is idempotent; a stale executor cannot publish a late response.
func (c *Client) RecordAXBinding(ctx context.Context, id string, operationID, executorID uuid.UUID, binding AXBinding) error {
	for _, ref := range []*ax.ResourceRef{binding.Task, binding.Group, binding.PreparedRuntime} {
		if err := ax.ValidateRef(ref, true); err != nil {
			return err
		}
		if ref.Atespace != binding.Task.Atespace {
			return fmt.Errorf("cross-atespace AX binding")
		}
	}
	if operationID == uuid.Nil || executorID == uuid.Nil {
		return ErrConflict
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		var currentOperation, currentExecutor *uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT operation_id, executor_id FROM runtime_instance WHERE id = $1 FOR UPDATE`, id).Scan(&currentOperation, &currentExecutor); err != nil {
			return notFoundOr(err)
		}
		if currentOperation == nil || currentExecutor == nil || *currentOperation != operationID || *currentExecutor != executorID {
			return ErrConflict
		}
		_, err := tx.Exec(ctx, `INSERT INTO ax_runtime_binding
   (runtime_id,atespace,task_name,task_uid,group_name,group_uid,prepared_runtime_name,prepared_runtime_uid,creation_operation_id)
   VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) ON CONFLICT (runtime_id) DO NOTHING`, id, binding.Task.Atespace, binding.Task.Name, binding.Task.Uid, binding.Group.Name, binding.Group.Uid, binding.PreparedRuntime.Name, binding.PreparedRuntime.Uid, operationID)
		if err != nil {
			return err
		}
		existing, err := readAXBinding(ctx, tx, id)
		if err != nil {
			return err
		}
		if !proto.Equal(existing.Task, binding.Task) || !proto.Equal(existing.Group, binding.Group) || !proto.Equal(existing.PreparedRuntime, binding.PreparedRuntime) {
			return fmt.Errorf("AX runtime binding is immutable: %w", ErrConflict)
		}
		return nil
	})
}

// AXCreation survives a later delete/TTL claim. Its original operation identity
// is needed to identify a delayed creation result without adopting another Task.
type AXCreation struct {
	OperationID         uuid.UUID
	Atespace            string
	TaskName            string
	PreparedRuntimeName string
	PreparedRuntimeUID  string
}

func (c *Client) GetAXCreation(ctx context.Context, id string) (AXCreation, error) {
	row, err := queryOne(ctx, c.db, `SELECT operation_id,atespace,task_name,prepared_runtime_name,prepared_runtime_uid FROM ax_runtime_creation WHERE runtime_id=$1`, pgx.RowToStructByName[AXCreation], id)
	return row, notFoundOr(err)
}
func (c *Client) ReserveAXCreation(ctx context.Context, id string, operationID, executorID uuid.UUID, task, runtime *ax.ResourceRef) error {
	if err := ax.ValidateRef(task, false); err != nil {
		return err
	}
	if err := ax.ValidateRef(runtime, true); err != nil {
		return err
	}
	if task.Atespace != runtime.Atespace {
		return ErrConflict
	}
	return c.withTx(ctx, func(tx pgx.Tx) error {
		var currentOperation, currentExecutor *uuid.UUID
		if err := tx.QueryRow(ctx, `SELECT operation_id,executor_id FROM runtime_instance WHERE id=$1 FOR UPDATE`, id).Scan(&currentOperation, &currentExecutor); err != nil {
			return notFoundOr(err)
		}
		if currentOperation == nil || currentExecutor == nil || *currentOperation != operationID || *currentExecutor != executorID {
			return ErrConflict
		}
		_, err := tx.Exec(ctx, `INSERT INTO ax_runtime_creation (runtime_id,operation_id,atespace,task_name,prepared_runtime_name,prepared_runtime_uid) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (runtime_id) DO NOTHING`, id, operationID, task.Atespace, task.Name, runtime.Name, runtime.Uid)
		if err != nil {
			return err
		}
		row, err := queryOne(ctx, tx, `SELECT operation_id,atespace,task_name,prepared_runtime_name,prepared_runtime_uid FROM ax_runtime_creation WHERE runtime_id=$1`, pgx.RowToStructByName[AXCreation], id)
		if err != nil {
			return err
		}
		if row.OperationID != operationID || row.Atespace != task.Atespace || row.TaskName != task.Name || row.PreparedRuntimeName != runtime.Name || row.PreparedRuntimeUID != runtime.Uid {
			return ErrConflict
		}
		return nil
	})
}
