# RProc - Reddit Data File Processor

RProc is a command-line tool for processing Reddit data dumps in zstd-compressed NDJSON format. It provides capabilities for filtering specific subreddit content and converting data to CSV format.

## Features

- Filter Reddit submissions and comments by field values
- Convert Reddit data to CSV format
- Process large zstd-compressed files efficiently
- Support for parallel processing
- Progress tracking and detailed logging
- Filter using exact match, partial match, or regex patterns

## Installation

```bash
go install github.com/acquisitionist/rproc@latest
```

Or clone and build from source:
```bash
git clone https://github.com/acquisitionist/rproc.git
cd rproc
go build
```

## Quick Start

### Filter Submissions from a Subreddit

```bash
# Get all posts from r/wallstreetbets
rproc filter ./reddit_data ./output --field subreddit --value wallstreetbets

# Use regex matching
rproc filter ./reddit_data ./output --field subreddit --value "bitcoin.*" --regex

# Use partial matching
rproc filter ./reddit_data ./output --field subreddit --value "crypto" --partial
```

### Convert to CSV

```bash
# Convert submissions to CSV
rproc csv ./reddit_data ./output
```

## Common Use Cases

### Filter Submissions by Field
```bash
# Get all posts by a specific author
rproc filter ./input ./output --field author --value "spez"

# Get posts with specific words in title
rproc filter ./input ./output --field title --value "announcement" --partial

# Get posts from multiple subreddits (using a file)
echo "wallstreetbets\nbitcoin" > subreddits.txt
rproc filter ./input ./output --field subreddit --value-list subreddits.txt
```

### Processing Large Datasets
```bash
# Use multiple threads for faster processing
rproc filter ./input ./output --field subreddit --value wallstreetbets --threads 4

# Only process specific date ranges
rproc filter ./input ./output --field subreddit --value wallstreetbets --file-filter "RS_2023-.*"
```

## Available Fields

For filtering, you can use these fields:
- `subreddit` - Subreddit name
- `author` - Post author username
- `title` - Post title (submissions only)
- `selftext` - Post content (submissions only)
- `body` - Comment content (comments only)
- `domain` - Link domain (submissions only)

## CSV Output Fields

### Submissions (RS_*.zst files)
- author
- title
- score
- created
- link
- text
- url

### Comments (RC_*.zst files)
- author
- score
- created
- link
- body

## Command Reference

### Global Flags
- `--debug` - Enable debug logging
- `--threads` - Number of processing threads (default: 1)
- `--file-filter` - Regex for matching input filenames (default: ".*")

### Filter Command Flags
- `--field` - Field to filter on (required)
- `--value` - Value to match against (required unless using --value-list)
- `--value-list` - File containing newline-separated values to match
- `--partial` - Use partial string matching
- `--regex` - Use regex matching
- `--error-rate` - Acceptable percentage of errors (0-100)

## Data File Format

RProc expects Reddit data files in the following format:
- Submissions: Files starting with `RS_` (e.g., `RS_2023-01.zst`)
- Comments: Files starting with `RC_` (e.g., `RC_2023-01.zst`)
- Compressed using zstd
- Each line contains a JSON object

## Tips and Troubleshooting

- Use `--debug` for detailed logging if you encounter issues
- File patterns use regex: `RS_2023-.*` matches all 2023 submission files
- Memory usage scales with thread count - start low and increase if needed
- Use partial matching (`--partial`) for more flexible text searches
- Check log output for progress and error information

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

[MIT License](LICENSE)
