package database

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
	"caching-api/internal/config"
)

type Database interface {
	Get(key string) (map[string]any, error)
	Set(key string, data map[string]any) error
	Update(key string, data map[string]any) error
	Delete(key string) error
	Close()
	GetConnection() *sql.DB
}

type PostgreSQL struct {
	db *sql.DB
}

func NewPostgreSQL(cfg config.DatabaseConfig) *PostgreSQL {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DBName, cfg.SSLMode)
	
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)
	
	if err := db.Ping(); err != nil {
		log.Fatal(err)
	}
	
	createTables(db)
	setupTriggers(db)
	
	return &PostgreSQL{db: db}
}

func (p *PostgreSQL) Get(key string) (map[string]any, error) {
	query := `SELECT id, data, version FROM cacheable_data WHERE id = $1`
	row := p.db.QueryRow(query, key)
	
	var id string
	var data string
	var version int
	err := row.Scan(&id, &data, &version)
	if err != nil {
		return nil, err
	}
	
	return map[string]any{
		"id":      id,
		"data":    data,
		"version": version,
	}, nil
}

func (p *PostgreSQL) Set(key string, data map[string]any) error {
	query := `INSERT INTO cacheable_data (id, data, version) VALUES ($1, $2, $3) 
	          ON CONFLICT (id) DO UPDATE SET data = $2, version = cacheable_data.version + 1`
	
	_, err := p.db.Exec(query, key, data["data"], 1)
	return err
}

func (p *PostgreSQL) Update(key string, data map[string]any) error {
	query := `UPDATE cacheable_data SET data = $1, version = version + 1, updated_at = NOW() 
	          WHERE id = $2 AND version = $3`
	
	result, err := p.db.Exec(query, data["data"], key, data["version"])
	if err != nil {
		return err
	}
	
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return fmt.Errorf("optimistic locking conflict")
	}
	
	return nil
}

func (p *PostgreSQL) Delete(key string) error {
	query := `DELETE FROM cacheable_data WHERE id = $1`
	_, err := p.db.Exec(query, key)
	return err
}

func (p *PostgreSQL) Close() {
	p.db.Close()
}

func (p *PostgreSQL) GetConnection() *sql.DB {
	return p.db
}

func createTables(db *sql.DB) {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS cacheable_data (
			id VARCHAR(255) PRIMARY KEY,
			data TEXT NOT NULL,
			version INTEGER DEFAULT 1,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		)`,
		`CREATE TABLE IF NOT EXISTS cache_invalidation_log (
			id SERIAL PRIMARY KEY,
			operation VARCHAR(50),
			table_name VARCHAR(100),
			record_id VARCHAR(255),
			old_data JSONB,
			new_data JSONB,
			created_at TIMESTAMP DEFAULT NOW()
		)`,
	}
	
	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			log.Printf("Error creating table: %v", err)
		}
	}
}

func setupTriggers(db *sql.DB) {
	triggerSQL := `
	CREATE OR REPLACE FUNCTION cache_invalidation_trigger()
	RETURNS TRIGGER AS $$
	BEGIN
		INSERT INTO cache_invalidation_log (operation, table_name, record_id, old_data, new_data)
		VALUES (TG_OP, TG_TABLE_NAME, 
			CASE 
				WHEN TG_OP = 'DELETE' THEN OLD.id::text 
				ELSE NEW.id::text 
			END,
			to_jsonb(OLD),
			to_jsonb(NEW)
		);
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;
	
	DROP TRIGGER IF EXISTS cache_data_change_trigger ON cacheable_data;
	CREATE TRIGGER cache_data_change_trigger
	AFTER INSERT OR UPDATE OR DELETE ON cacheable_data
	FOR EACH ROW EXECUTE FUNCTION cache_invalidation_trigger();
	`
	
	_, err := db.Exec(triggerSQL)
	if err != nil {
		log.Printf("Error setting up triggers: %v", err)
	}
}