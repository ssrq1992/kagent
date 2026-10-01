-- +goose Up
-- Migration 000002 already rejects legacy instance data. These are lossless
-- column renames: business IDs, history positions, foreign keys and transactions
-- stay intact. Runtime references contain AX-owned boundary/checkpoint JSON.
ALTER TABLE runtime_revision RENAME COLUMN actor_template_atespace TO prepared_runtime_atespace;
ALTER TABLE runtime_revision RENAME COLUMN actor_template_name TO prepared_runtime_name;
ALTER TABLE runtime_revision RENAME COLUMN actor_template_uid TO prepared_runtime_uid;
ALTER TABLE session RENAME COLUMN actor_uid TO task_uid;
ALTER TABLE session_checkpoint RENAME COLUMN snapshot_atespace TO runtime_atespace;
ALTER TABLE session_checkpoint RENAME COLUMN snapshot_uri TO runtime_reference;
ALTER TABLE session_task RENAME COLUMN snapshot_atespace TO runtime_atespace;
ALTER TABLE session_task RENAME COLUMN snapshot_uri TO runtime_reference;
ALTER TABLE session_task_event RENAME COLUMN snapshot_atespace TO runtime_atespace;
ALTER TABLE session_task_event RENAME COLUMN snapshot_uri TO runtime_reference;
ALTER TABLE session_checkpoint RENAME COLUMN tag_uid TO checkpoint_uid;
ALTER VIEW agent_runtime_revision RENAME COLUMN actor_template_atespace TO prepared_runtime_atespace;
ALTER VIEW agent_runtime_revision RENAME COLUMN actor_template_name TO prepared_runtime_name;
ALTER VIEW agent_runtime_revision RENAME COLUMN actor_template_uid TO prepared_runtime_uid;
ALTER VIEW session_record RENAME COLUMN actor_uid TO task_uid;

-- +goose Down
ALTER VIEW session_record RENAME COLUMN task_uid TO actor_uid;
ALTER VIEW agent_runtime_revision RENAME COLUMN prepared_runtime_uid TO actor_template_uid;
ALTER VIEW agent_runtime_revision RENAME COLUMN prepared_runtime_name TO actor_template_name;
ALTER VIEW agent_runtime_revision RENAME COLUMN prepared_runtime_atespace TO actor_template_atespace;
ALTER TABLE session_checkpoint RENAME COLUMN checkpoint_uid TO tag_uid;
ALTER TABLE session_task_event RENAME COLUMN runtime_reference TO snapshot_uri;
ALTER TABLE session_task_event RENAME COLUMN runtime_atespace TO snapshot_atespace;
ALTER TABLE session_task RENAME COLUMN runtime_reference TO snapshot_uri;
ALTER TABLE session_task RENAME COLUMN runtime_atespace TO snapshot_atespace;
ALTER TABLE session_checkpoint RENAME COLUMN runtime_reference TO snapshot_uri;
ALTER TABLE session_checkpoint RENAME COLUMN runtime_atespace TO snapshot_atespace;
ALTER TABLE session RENAME COLUMN task_uid TO actor_uid;
ALTER TABLE runtime_revision RENAME COLUMN prepared_runtime_uid TO actor_template_uid;
ALTER TABLE runtime_revision RENAME COLUMN prepared_runtime_name TO actor_template_name;
ALTER TABLE runtime_revision RENAME COLUMN prepared_runtime_atespace TO actor_template_atespace;
