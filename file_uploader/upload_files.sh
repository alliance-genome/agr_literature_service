#!/usr/bin/env bash

MOD=$1

# request okta access token
OKTA_ACCESS_TOKEN=$(curl -s --request POST --url https://${OKTA_DOMAIN}/v1/token \
  --header 'accept: application/json' \
  --header 'cache-control: no-cache' \
  --header 'content-type: application/x-www-form-urlencoded' \
  --data "grant_type=client_credentials&scope=admin&client_id=${OKTA_CLIENT_ID}&client_secret=${OKTA_CLIENT_SECRET}" \
    | jq '.access_token' | tr -d '"')

upload_file () {
  url="http://${API_SERVER}:${API_PORT}/reference/referencefile/file_upload/?reference_curie=${reference_id}&display_name=${display_name}&file_class=${file_class}&file_publication_status=${file_publication_status}&file_extension=${file_extension}&is_annotation=false"
  if [[ ${pdf_type} != "null" ]]; then
    url="${url}&pdf_type=${pdf_type}"
  fi
  curl --request POST --url ${url} \
    -H 'accept: application/json' \
    -H "Authorization: Bearer ${OKTA_ACCESS_TOKEN}" \
    -H 'Content-Type: multipart/form-data' \
    -F "file=@${filepath};type=text/plain" \
    -F 'metadata_file='
}

extract_metadata() {
  filepath=$1
  filename=$(basename ${filepath})
  display_name="${filename%.*}"
  file_extension="${filename##*.}"
  file_class="main"
  file_publication_status="final"
  pdf_type="null"

  regex="^([0-9]+)[_]([^_]+)[_]?(.*)?\..*$"
  [[ $filename =~ $regex ]]
  reference_id=${BASH_REMATCH[1]}
  author_and_year=${BASH_REMATCH[2]}
  additional_options=${BASH_REMATCH[3]}
  if [[ "${additional_options}" == "temp" ]]; then
    file_publication_status="temp"
  elif [[ "${additional_options,,}" == "supp" ]]; then
    file_class="supplement"
  elif [[ "${additional_options,,}" == "aut" ]]; then
    pdf_type="aut"
  elif [[ "${additional_options,,}" == "ocr" ]]; then
    pdf_type="ocr"
  elif [[ "${additional_options,,}" == "html" ]]; then
    pdf_type="html"
  fi
}

for refdir in /usr/files_to_upload/*; do
  if [[ -d ${refdir} ]]; then
    echo "Processing supplemental files for reference ${refdir}"
    for reffile in ${refdir}/*; do
      if [[ ! -d ${reffile} && $(basename ${reffile}) != "*" ]]; then
        echo "Processing supplemental file ${reffile}"
        extract_metadata $reffile
        file_class="supplement"
        if [[ $(basename ${refdir}) =~ ^[0-9]{10}$ ]]; then
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
      else
        echo "Cannot process reference ${refdir}"
      fi
    done
  fi
done