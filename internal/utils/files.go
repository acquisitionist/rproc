package utils

import (
	"errors"
	"fmt"
	c "github.com/acquisitionist/rproc/internal/common"
	"github.com/rs/zerolog/log"
	"github.com/spf13/afero"
	"os"
	"path/filepath"
	"regexp"
)

var (
	ErrNoInput = errors.New("no input files found")
)

func GetInputFiles(deps *c.Dependencies, inputDir string, fileFilter string, debug bool) ([]string, error) {
	var files []string
	var totalFiles int

	filterRegex := regexp.MustCompile(fileFilter)

	log.Info().
		Str("pattern", fileFilter).
		Str("input_dir", inputDir).
		Msg("Looking for input files")

	err := afero.Walk(deps.Fs, inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		totalFiles++

		fileName := info.Name()
		if filepath.Ext(fileName) != ".zst" {
			if debug {
				log.Debug().
					Str("file", fileName).
					Msg("Skipping file - not a .zst file")
			}
			return nil
		}
		if !filterRegex.MatchString(fileName) {
			if debug {
				log.Debug().
					Str("file", fileName).
					Msg("Skipping file - does not match filter")
			}
			return nil
		}

		files = append(files, path)

		if debug {
			log.Debug().
				Str("file", fileName).
				Msg("Found matching file")
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("walking input directory: %w", err)
	}

	if len(files) == 0 {
		return nil, ErrNoInput
	}

	log.Info().
		Int("total_files", totalFiles).
		Int("matching_files", len(files)).
		Str("pattern", fileFilter).
		Msg("File filtering complete")

	return files, nil
}
