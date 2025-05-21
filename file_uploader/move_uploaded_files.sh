#!/usr/bin/env bash

SOURCE_DIR=$1 # Source directory passed as an argument
DEST_DIR=$2   # Destination directory passed as an argument
UPLOAD_LOG=$3 # Upload log file passed as an argument

if [[ -z "$SOURCE_DIR" || -z "$DEST_DIR" || -z "$UPLOAD_LOG" ]]; then
  echo "ERROR: Usage: $0 <source_dir> <dest_dir> <upload_log>"
  exit 1
fi

if [[ ! -d "$SOURCE_DIR" ]]; then
  echo "ERROR: Source directory '$SOURCE_DIR' does not exist."
  exit 1
fi

if [[ ! -f "$UPLOAD_LOG" ]]; then
  echo "ERROR: Upload log file '$UPLOAD_LOG' does not exist."
  exit 1
fi

if [[ ! -d "$DEST_DIR" ]]; then
  echo "Creating destination directory: $DEST_DIR"
  mkdir -p "$DEST_DIR"
fi

move_file() {
  local file_path=$1
  local relative_path=${file_path#"$SOURCE_DIR/"}
  local dest_path="$DEST_DIR/$relative_path"
  local dest_dir=$(dirname "$dest_path")

  if [[ ! -d "$dest_dir" ]]; then
    mkdir -p "$dest_dir"
  fi

  mv "$file_path" "$dest_path"
  echo "Moved: $file_path -> $dest_path"
}

process_directory() {
  local dir_path=$1

  for file in "$dir_path"/*; do
    if [[ -f "$file" ]]; then
      # Check upload status from the log
      upload_status=$(grep -F "FILE: ${file}" "$UPLOAD_LOG" | grep -oP 'API_CALL_STATUS: \K\w+')

      if [[ "$upload_status" == "success" ]]; then
        move_file "$file"
      fi
    elif [[ -d "$file" ]]; then
      process_directory "$file"
    fi
  done

  # Remove empty directories
  if [[ -z "$(ls -A "$dir_path")" ]]; then
    rmdir "$dir_path"
    echo "Removed empty directory: $dir_path"
  fi
}

# Start processing the source directory
process_directory "$SOURCE_DIR"