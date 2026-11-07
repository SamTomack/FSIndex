package indexer

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log/slog"
	"path"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// TODO: Should make configurable
var RootDirPath string = "C:\\ProgramData\\FSIndex"
var DbPath string = path.Join(RootDirPath, "index.db")
var LogFile string = path.Join(RootDirPath, "log.txt")
var RootScanDirectory = "C:\\Program Files"

const batchSize int = 5000
const ServiceName = "FSIndex"

type RawInfo struct {
	Path string
	Info fs.FileInfo
}

type FileMeta struct {
	Name         string
	Path         string
	Extension    string
	Size         int64
	LastModified time.Time
}

func SearchFile(db *sql.DB, filePath string) (string, error) {
	var output string
	row := db.QueryRow("SELECT file_name FROM files WHERE file_path = ?", filePath)
	err := row.Scan(&output)
	if err != nil {
		return "", err
	}

	return output, nil
}

func InitDB(logger *slog.Logger) (*sql.DB, error) {
	db, err := sql.Open("sqlite", DbPath)
	if err != nil {
		logger.Error("Failed to open database " + err.Error())
		return nil, err
	}

	//Write ahead for efficiency
	_, err = db.Exec(`
		PRAGMA journal_mode = WAL;
		PRAGMA synchronous = NORMAL;
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

func WriteFromFilemetaChannel(db *sql.DB, ch <-chan FileMeta, logger *slog.Logger) error {
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
		defer statement.Close()
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

func ScanLoop() error {
	err := EnsurePersistentFiles()
	if err != nil {
		return err
	}

	logger, file, err := InitLogging()
	if err != nil {
		return err
	}

	defer file.Close()
	logger.Info("Entering scan loop.")

	err = Scan(logger)
	if err != nil {
		logger.Error("Encountered an unhandled error during directory scan: " + err.Error())
	}

	return nil
}

func ReceivePaths(inCh <-chan RawInfo, outCh chan<- FileMeta, logger *slog.Logger) {
	for rawInfo := range inCh {
		var name string
		var ext string
		var size int64

		if rawInfo.Info.IsDir() {
			name = ""
			ext = ""
			size = -1
		} else {
			name = rawInfo.Info.Name()
			ext = filepath.Ext(name)
			size = rawInfo.Info.Size()
		}

		meta := FileMeta{
			Name:         name,
			Path:         rawInfo.Path,
			Extension:    ext,
			LastModified: rawInfo.Info.ModTime(),
			Size:         size,
		}

		outCh <- meta
	}

	close(outCh)
}

func Scan(logger *slog.Logger) error {
	rawInfoCh := make(chan RawInfo, 1000)
	metaCh := make(chan FileMeta, 1000)

	db, err := InitDB(logger)
	if err != nil {
		logger.Error("Failed to open database: " + err.Error())
		return err
	}

	defer db.Close()

	go GetPaths(RootScanDirectory, rawInfoCh, logger)
	go ReceivePaths(rawInfoCh, metaCh, logger)

	err = WriteFromFilemetaChannel(db, metaCh, logger)
	if err != nil {
		fmt.Println("Failed to write to DB: " + err.Error())
		return err
	}

	return nil
}

func GetPaths(root string, ch chan<- RawInfo, logger *slog.Logger) {
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			logger.Warn(fmt.Sprintf("Failed to walk %s: %s", d.Name(), err.Error()))
		}

		info, err := d.Info()
		if err != nil {
			logger.Warn("Successfully walked %s, but failed to retrieve file info: %s", d.Name(), err.Error())
		}

		ch <- RawInfo{Path: path, Info: info}

		count++
		if count%500 == 0 {
			count = 0
			time.Sleep(10 * time.Millisecond)
		}

		return nil
	})

	if err != nil {
		slog.Warn("Failed to walk root %s,: %s", root, err.Error())
	}

	close(ch)
}

func Debug_Get(db *sql.DB, num int, logger *slog.Logger) error {
	var output string

	rows, err := db.Query(fmt.Sprintf("SELECT file_path FROM files LIMIT %d;", num))
	if err != nil {
		return err
	}

	defer rows.Close()

	for rows.Next() {
		err = rows.Scan(&output)
		logger.Debug(output)
		if err != nil {
			return err
		}
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	return nil
}
