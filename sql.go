package indexer

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"
)

const maxListSize = 100

func SearchFileNameExact(db *sql.DB, fileName string, logger *slog.Logger) ([maxListSize]FileMeta, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, "SELECT * FROM files WHERE file_name = ?", fileName)
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to execute SearchFileNameExact query: %s"), fileName)
		return [maxListSize]FileMeta{}, err
	}

	defer rows.Close()
	output := [maxListSize]FileMeta{}
	count := 0
	for rows.Next() {
		var temp FileMeta
		err = rows.Scan(&temp)
		if err != nil {
			logger.Error("Failed to scan row as FileMeta.")
			return [maxListSize]FileMeta{}, err
		}

		output[count] = temp
		if count == maxListSize {
			break
		}
	}

	return output, nil
}

func SearchFileNameSegment(db *sql.DB, fileName string, logger *slog.Logger) ([maxListSize]FileMeta, error) {
	// TODO: restrict queries by invalid windows file names
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM files WHERE file_name MATCH %s%", fileName))
	if err != nil {
		logger.Error(fmt.Sprintf("Failed to execute SearchFileNameSegment query: %s"), fileName)
		return [maxListSize]FileMeta{}, err
	}

	defer rows.Close()
	output := [maxListSize]FileMeta{}
	count := 0
	for rows.Next() {
		var temp FileMeta
		err = rows.Scan(&temp)
		if err != nil {
			logger.Error("Failed to scan row as FileMeta.")
			return [maxListSize]FileMeta{}, err
		}

		output[count] = temp
		if count == maxListSize {
			break
		}
	}

	return output, nil
}

func InitDB(logger *slog.Logger) (*sql.DB, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		logger.Error("Failed to open database " + err.Error())
		return nil, err
	}

	db.SetMaxOpenConns(1)

	//Write ahead for efficiency
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
		PRAGMA cache_size = -20000;
		PRAGMA temp_store = MEMORY;
		CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_path TEXT UNIQUE,
			file_name TEXT,
			file_size INTEGER,
			file_extension TEXT,
			last_modified TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_file_name ON files(file_name);
	`)

	if err != nil {
		logger.Error("Failed to initialize SQL database " + err.Error())
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func writeFromFilemetaChannel(db *sql.DB, ch <-chan FileMeta, logger *slog.Logger) error {
	query := `
		INSERT INTO files (file_path, file_name, file_size, file_extension, last_modified)
		VALUES(?, ?, ?, ?, ?)
		ON CONFLICT(file_path) DO UPDATE SET
			file_size = excluded.file_size,
			last_modified = excluded.last_modified,
			file_extension = excluded.file_extension;
	`
	channelClosed := false

	for {
		transaction, err := db.Begin()
		if err != nil {
			logger.Error("Failed to begin SQL transaction")
			return err
		}

		statement, err := transaction.Prepare(query)
		if err != nil {
			transaction.Rollback()
			logger.Error(fmt.Sprintf("Failed to prepare transaction: %s", err.Error()))
			return err
		}

		statement.Close()
		count := 0

		for i := 0; i < batchSize; i++ {
			meta, ok := <-ch
			if !ok {
				channelClosed = true
				break
			}

			_, err = statement.Exec(meta.Path, meta.Name, meta.Size, meta.Extension, meta.LastModified)
			if err != nil {
				logger.Error(fmt.Sprintf("Failed to insert SQL: %s", err.Error()))
				transaction.Rollback()
				return err
			}

			count++
		}

		if count > 0 {
			transaction.Commit()
			logger.Info(fmt.Sprintf("Inserted %d records", count))
		}

		// If the channel was closed, we are done. Exit the outer loop.
		if channelClosed {
			logger.Info("Scan completed successfully.")
			break
		}
	}

	return nil
}
