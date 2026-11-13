package indexer

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// TODO: Should make configurable
var rootDirPath string = "C:\\ProgramData\\FSIndex"
var dbPath string = filepath.Join(rootDirPath, "index.db")
var logFile string = filepath.Join(rootDirPath, "log.txt")
var rootScanDirectory = "C:\\"

const batchSize int = 5000
const ServiceName = "FSIndex"

// TODO: Should make configurable
var skipDirs = map[string]bool{
	"windows":                   true,
	"$recycle.bin":              true,
	"system volume information": true,
	"recovery":                  true,
	"msocache":                  true,
}

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

func Scan(svcCtx context.Context, logger *slog.Logger) error {
	err := scan(svcCtx, logger)
	if err != nil {
		logger.Error("Encountered an unhandled error during directory scan: " + err.Error())
	}

	return nil
}

func receivePaths(inCh <-chan RawInfo, outCh chan<- FileMeta) {
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

func scan(svcCtx context.Context, logger *slog.Logger) error {
	rawInfoCh := make(chan RawInfo, 1000)
	metaCh := make(chan FileMeta, 1000)

	db, err := InitDB(logger)
	if err != nil {
		logger.Error("Failed to open database: " + err.Error())
		return err
	}

	defer db.Close()

	go getPaths(rootScanDirectory, rawInfoCh, svcCtx, logger)
	go receivePaths(rawInfoCh, metaCh)

	err = writeFromFilemetaChannel(db, metaCh, logger)
	if err != nil {
		fmt.Println("Failed to write to DB: " + err.Error())
		return err
	}

	return nil
}

func getPaths(root string, ch chan<- RawInfo, svcCtx context.Context, logger *slog.Logger) {
	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		//Check for cancellation on every iteration
		select {
		case <-svcCtx.Done():
			return filepath.SkipAll
		default:
		}

		if err != nil {
			if d != nil {
				logger.Warn(fmt.Sprintf("Failed to walk %s: %s", d.Name(), err.Error()))
			} else {
				logger.Warn(fmt.Sprintf("Failed to access %s: %s", path, err.Error()))
			}

			return filepath.SkipDir
		}

		if skipDirs[strings.ToLower(d.Name())] {
			return filepath.SkipDir
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
