package indexer

import (
	"errors"
	"log/slog"
	"os"
)

// TODO: rolling log
func InitLogging() (*slog.Logger, *os.File, error) {
	file, err := os.OpenFile(LogFile, os.O_WRONLY|os.O_APPEND, os.ModeAppend)
	if err != nil {
		return nil, nil, err
	}

	handler := slog.NewJSONHandler(file, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	})

	logger := slog.New(handler)

	return logger, file, nil
}

func EnsurePersistentFiles() error {
	err := InitDir(RootDirPath)
	if err != nil {
		return err
	}

	err = InitFile(LogFile)
	if err != nil {
		return err
	}

	err = InitFile(DbPath)
	if err != nil {
		return err
	}

	return nil
}

func InitDir(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(path, 0755)
			if err != nil {
				return err
			}
		}

		return err
	}

	return nil
}

func InitFile(path string) error {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			file, err := os.Create(path)
			if err != nil {
				return err
			}

			defer file.Close()
			return nil
		}

		return err
	}

	return nil
}
