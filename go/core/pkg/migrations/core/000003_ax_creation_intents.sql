-- +goose Up
CREATE TABLE ax_runtime_creation (
    runtime_id UUID PRIMARY KEY REFERENCES runtime_instance(id) ON DELETE CASCADE,
    operation_id UUID NOT NULL,
    atespace TEXT NOT NULL,
    task_name TEXT NOT NULL,
    prepared_runtime_name TEXT NOT NULL,
    prepared_runtime_uid TEXT NOT NULL,
    UNIQUE (atespace, task_name)
);

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM ax_runtime_creation) THEN
  RAISE EXCEPTION 'cannot remove unresolved AX creation identities';
 END IF;
END $$;
-- +goose StatementEnd
DROP TABLE ax_runtime_creation;
