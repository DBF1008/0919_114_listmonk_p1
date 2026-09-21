package migrations

import (
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/knadh/koanf/v2"
	"github.com/knadh/stuffbin"
)

func V6_3_0(db *sqlx.DB, fs stuffbin.FileSystem, ko *koanf.Koanf, lo *log.Logger) error {
	// Add the pause_reason column that records why a campaign was paused
	// (eg: auto-paused after exceeding the send error threshold).
	if _, err := db.Exec(`
		ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS pause_reason TEXT NULL;
	`); err != nil {
		return err
	}

	return nil
}
