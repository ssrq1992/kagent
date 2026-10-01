package migrations

import (
	"context"
	"database/sql"
	"testing"

	"github.com/pressly/goose/v3"
)

func TestAXMigrationRejectsExistingRuntimeState(t *testing.T) {
	dsn := startTestDB(t)
	source := BuiltinSources(false)[0]
	err := WithProvider(t.Context(), dsn, source, func(p *goose.Provider) error { _, err := p.UpTo(t.Context(), 1); return err })
	if err != nil {
		t.Fatal(err)
	}
	execSQL(t, dsn, `INSERT INTO runtime_instance(id,kind,user_id,request_id,state,operation)
 VALUES ('00000000-0000-0000-0000-000000000001','sandbox','user','old','RUNTIME_STATE_CREATING','RUNTIME_OPERATION_CREATE')`)
	if err = RunUp(context.Background(), dsn, []Source{source}); err == nil {
		t.Fatal("migration adopted an old runtime")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err = db.QueryRowContext(t.Context(), "SELECT count(*) FROM runtime_instance").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatal("migration discarded existing runtime state")
	}
}
