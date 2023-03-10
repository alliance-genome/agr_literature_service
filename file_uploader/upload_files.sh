#!/usr/bin/env bash

# request okta access token
OKTA_ACCESS_TOKEN=$(curl -s --request POST --url https://${OKTA_DOMAIN}/v1/token \
  --header 'accept: application/json' \
  --header 'cache-control: no-cache' \
  --header 'content-type: application/x-www-form-urlencoded' \
  --data "grant_type=client_credentials&scope=admin&client_id=${OKTA_CLIENT_ID}&client_secret=${OKTA_CLIENT_SECRET}" \
    | jq '.access_token')

# just testing for now
echo $OKTA_ACCESS_TOKEN
exit

# for each file in folder, extract metadata and then send request to API with curl
curl -s --request POST --url https://${API_SERVER}:${API_PORT}/reference/referencefile/file_upload/?reference_curie=AGRKB:101000000000001&display_name=test&file_class=main&file_publication_status=final&file_extension=txt&pdf_type=null&is_annotation=false \
  -H 'accept: application/json' \
  -H "Authorization: Bearer ${OKTA_ACCESS_TOKEN}" \
  -H 'Content-Type: multipart/form-data' \
  -F "file=@test2.txt;type=text/plain" \
  -F 'metadata_file='
