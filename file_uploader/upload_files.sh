#!/usr/bin/env bash

MOD=$1

# request okta access token
generate_access_token () {
  OKTA_ACCESS_TOKEN=$(curl -s --request POST --url https://${OKTA_DOMAIN}/v1/token \
    --header 'accept: application/json' \
    --header 'cache-control: no-cache' \
    --header 'content-type: application/x-www-form-urlencoded' \
    --data "grant_type=client_credentials&scope=admin&client_id=${OKTA_CLIENT_ID}&client_secret=${OKTA_CLIENT_SECRET}" \
      | jq '.access_token' | tr -d '"')
}

urlencode () {
  printf %s "$1" | jq -sRr @uri
}

upload_file () {
  url="https://${API_SERVER}"
  if [[ ${API_PORT} != "" && ${API_PORT} != "80" ]]; then
    url="${url}:${API_PORT}"
  fi
  display_name=$(urlencode "${display_name}")
  url="${url}/reference/referencefile/file_upload/?reference_curie=${reference_id}&display_name=${display_name}&file_class=${file_class}&file_publication_status=${file_publication_status}&file_extension=${file_extension}&is_annotation=false&mod_abbreviation=${MOD}"
  if [[ ${pdf_type} != "null" ]]; then
    url="${url}&pdf_type=${pdf_type}"
  fi
  response=$(curl -s --request POST --url ${url} \
    -H 'accept: application/json' \
    -H "Authorization: Bearer ${OKTA_ACCESS_TOKEN}" \
    -H 'Content-Type: multipart/form-data' \
    -F "file=@\"${filepath}\";type=text/plain" \
    -F 'metadata_file=')
  if [[ $response == null ]]; then
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

extract_file_metadata() {
  filepath=$1
  filename=$(basename "${filepath}")
  display_name="${filename%.*}"
  file_extension="${filename##*.}"
  if [[ ${file_extension} == "pdf" ]]; then
    pdf_type="pdf"
  fi
}

parse_main_filename() {
  regex="^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$"
  [[ $filename =~ $regex ]]
  reference_id=${BASH_REMATCH[1]}
  author_and_year=${BASH_REMATCH[2]}
  additional_options=${BASH_REMATCH[3]}
  if [[ "${additional_options}" == "temp" ]]; then
    file_publication_status="temp"
  elif [[ "${additional_options,,}" == "aut" ]]; then
    pdf_type="aut"
  elif [[ "${additional_options,,}" == "ocr" ]]; then
    pdf_type="ocr"
  elif [[ "${additional_options,,}" == "html" ]]; then
    pdf_type="html"
  elif [[ "${additional_options,,}" == "lib" ]]; then
    pdf_type="lib"
  elif [[ "${additional_options,,}" == "tif" ]]; then
    pdf_type="tif"
  fi
}

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
    reference_id=$(basename $(dirname "${file_path}"))
  fi
  if [[ ${reference_id} =~ ^[0-9]{15}$ ]]; then
    reference_id="AGRKB:${reference_id}"
  elif [[ $MOD == "WB" ]]; then
    reference_id="WB:WBPaper${reference_id}"
  fi
  echo "reference ID: ${reference_id}"
  echo "display_name: ${display_name}"
  echo "file_extension: ${file_extension}"
  echo "file_class: ${file_class}"
  echo "file_publication_status: ${file_publication_status}"
  echo "pdf_type: ${pdf_type}"
  upload_file
}

generate_access_token

for reffileordir in /usr/files_to_upload/*; do
  if [[ -d ${reffileordir} ]]; then
    echo "Processing supplemental files from ${reffileordir}"
    for reffile in ${reffileordir}/*; do
      if [[ ! -d "${reffile}" && $(basename "${reffile}") != "*" ]]; then
        process_file "${reffile}" "supplement"
      else
        echo "Found empty dir or subdir for reference ${reffileordir}"
      fi
    done
  else
    process_file "${reffileordir}" "main"
  fi
done
