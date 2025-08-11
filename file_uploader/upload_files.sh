#!/usr/bin/env bash

MOD=$1
TEST_EXTRACTION=false
FILES_FOLDER="/usr/files_to_upload" # Default folder

# Check for the test extraction flag
if [[ "$2" == "--test-extraction" ]]; then
  TEST_EXTRACTION=true
  if [[ -n "$3" ]]; then
    FILES_FOLDER="$3"
  fi
elif [[ -n "$2" ]]; then
  FILES_FOLDER="$2"
fi

# Validate the folder exists
if [[ ! -d "$FILES_FOLDER" ]]; then
  echo "ERROR: Folder '$FILES_FOLDER' does not exist."
  exit 1
fi

# request okta access token
generate_access_token () {
  OKTA_ACCESS_TOKEN=$(curl -s --request POST --url https://${OKTA_DOMAIN}/v1/token \
    --header 'accept: application/json' \
    --header 'cache-control: no-cache' \
    --header 'content-type: application/x-www-form-urlencoded' \
    --data "grant_type=client_credentials&scope=admin&client_id=${OKTA_CLIENT_ID}&client_secret=${OKTA_CLIENT_SECRET}" \
      | jq '.access_token' | tr -d '"')
}

export -f generate_access_token

urlencode () {
  printf %s "$1" | jq -sRr @uri
}

export -f urlencode

upload_file () {
  url="https://${API_SERVER}"
  if [[ ${API_PORT} != "" && ${API_PORT} != "80" ]]; then
    url="${url}:${API_PORT}"
  fi
  display_name=$(urlencode "${display_name}")
  url="${url}/reference/referencefile/file_upload/?reference_curie=${reference_id}&display_name=${display_name}&file_class=${file_class}&file_publication_status=${file_publication_status}&file_extension=${file_extension}&is_annotation=false&mod_abbreviation=${MOD}&upload_if_already_converted=true"
  if [[ ${pdf_type} != "null" ]]; then
    url="${url}&pdf_type=${pdf_type}"
  fi
  response=$(curl -s --request POST --url ${url} \
    -H 'accept: application/json' \
    -H "Authorization: Bearer ${OKTA_ACCESS_TOKEN}" \
    -H 'Content-Type: multipart/form-data' \
    -F "file=@\"${filepath}\";type=text/plain")

  # Check if the response contains a "detail" field with specific phrases
  detail_message=$(echo "$response" | jq -r '.detail // empty')

  if [[ ("${detail_message}" == *"is currently in progress"* && "${detail_message}" == *"process is complete before uploading any files"*) || "${detail_message}" == *"Curated topic and entity tags or automated tags generated from your MOD"* ]]; then
    echo "INFO: ${detail_message}"
    return
  fi

  if [[ "${response}" == "\"success\"" ]]; then
    upload_status="success"
    response="empty response"
  else
    upload_status="error"
  fi
  if [[ "${response}" == "{\"detail\":\"Expired token\"}" ]]; then
    echo "INFO: Access token expired, requesting a new one"
    generate_access_token
    upload_file
  else
    echo "API_CALL_STATUS: ${upload_status}, RESPONSE: ${response}, FILE: ${filepath}"
  fi
}

export -f upload_file

extract_file_metadata() {
  filepath=$1
  filename=$(basename "${filepath}")
  prefix=""
  if [[ "$filename" == "."* ]]; then
    prefix="."
    filename="${filename:1}"
  fi
  display_name="$prefix${filename%.*}"
  file_extension="${filename##*.}"
  if [[ "${filename}" == "${file_extension}" ]]; then
    file_extension=""
  fi
  if [[ ${file_extension} == "pdf" ]]; then
    pdf_type="pdf"
  fi
}

export -f extract_file_metadata

parse_main_filename() {
  regex_with_details="^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$"
  regex_numbers_only="^([0-9]+)\..*$"

  if [[ $filename =~ $regex_with_details ]]; then
    reference_id=${BASH_REMATCH[1]}
    author_and_year=${BASH_REMATCH[2]}
    additional_options=${BASH_REMATCH[3]}
  elif [[ $filename =~ $regex_numbers_only ]]; then
    reference_id=${BASH_REMATCH[1]}
    author_and_year=""
    additional_options=""
  else
    echo "ERROR: Filename does not match expected patterns."
    exit 1
  fi
  if [[ "${additional_options}" == "temp" ]]; then
    file_publication_status="temp"
  elif [[ "${additional_options,,}" == "aut" ]]; then
    pdf_type="aut"
  elif [[ "${additional_options,,}" == "ocr" ]]; then
    pdf_type="ocr"
  elif [[ "${additional_options,,}" == "html" ]]; then
    pdf_type="html"
  elif [[ "${additional_options,,}" == "htm" ]]; then
    pdf_type="html"
  elif [[ "${additional_options,,}" == "lib" ]]; then
    pdf_type="lib"
  elif [[ "${additional_options,,}" == "tif" ]]; then
    pdf_type="tif"
  fi
}

export -f parse_main_filename

process_file() {
  file_path=$1
  file_class=$2
  echo "Processing ${file_class} file ${file_path}"
  file_publication_status="final"
  pdf_type="null"
  extract_file_metadata "$file_path"
  if [[ ${file_class} == "main" ]]; then
    parse_main_filename
  else
    reference_id=$(echo "$file_path" | cut -d "/" -f4)
  fi
  if [[ ${reference_id} =~ ^[0-9]{15}$ ]]; then
    reference_id="AGRKB:${reference_id}"
  elif [[ $MOD == "WB" ]]; then
    reference_id="WB:WBPaper${reference_id}"
  elif [[ $MOD == "FB" ]]; then
    reference_id="PMID:${reference_id}"
  fi
  echo "reference ID: ${reference_id}"
  echo "display_name: ${display_name}"
  echo "file_extension: ${file_extension}"
  echo "file_class: ${file_class}"
  echo "file_publication_status: ${file_publication_status}"
  echo "pdf_type: ${pdf_type}"

  # Skip upload if testing extraction
  if [[ "$TEST_EXTRACTION" == false ]]; then
    upload_file
  else
    echo "TEST MODE: Skipping file upload."
  fi
}

export -f process_file

generate_access_token
export OKTA_ACCESS_TOKEN
export MOD

for reffileordir in "$FILES_FOLDER"/*; do
  if [[ -d ${reffileordir} ]]; then
    echo "Processing supplemental files from ${reffileordir}"
    find "${reffileordir}" -type f -exec bash -c 'process_file "$1" "supplement"' -- {} \;
  else
    process_file "${reffileordir}" "main"
  fi
done
