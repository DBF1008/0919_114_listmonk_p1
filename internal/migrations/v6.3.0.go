package migrations

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

// V6_3_0 performs the DB migrations for v6.3.0.
func V6_3_0(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf, lo *log.Logger) error {
	// Add the status_reason column that records why a campaign's status
	// changed (eg: auto-paused due to too many send errors).
	_, err := db.Exec(`
		ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS status_reason TEXT NULL;
	`)
	return err
}
