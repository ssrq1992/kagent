-- +goose Up
-- New installations only. Existing runtime state requires explicit adoption,
-- which this release intentionally does not implement. Keep all old data intact.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM runtime_instance)
       OR EXISTS (SELECT 1 FROM runtime_revision)
       OR EXISTS (SELECT 1 FROM session_checkpoint) THEN
        RAISE EXCEPTION 'AX runtime migration requires a new installation; existing runtime state cannot be adopted by this release';
    END IF;
END $$;
-- +goose StatementEnd

CREATE TABLE ax_runtime_binding (
    runtime_id UUID PRIMARY KEY REFERENCES runtime_instance(id) ON DELETE CASCADE,
    atespace TEXT NOT NULL CHECK (atespace <> ''),
    task_name TEXT NOT NULL CHECK (task_name <> ''),
    task_uid TEXT NOT NULL CHECK (task_uid <> ''),
    group_name TEXT NOT NULL CHECK (group_name <> ''),
    group_uid TEXT NOT NULL CHECK (group_uid <> ''),
    prepared_runtime_name TEXT NOT NULL CHECK (prepared_runtime_name <> ''),
    prepared_runtime_uid TEXT NOT NULL CHECK (prepared_runtime_uid <> ''),
    creation_operation_id UUID NOT NULL,
    UNIQUE (atespace, task_name),
    UNIQUE (task_uid)
);

-- +goose Down
-- Dropping live bindings would silently turn known instances into unbound names.
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM ax_runtime_binding) THEN
        RAISE EXCEPTION 'cannot remove AX runtime bindings while bound instances exist';
    END IF;
END $$;
-- +goose StatementEnd
DROP TABLE ax_runtime_binding;
