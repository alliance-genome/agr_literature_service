FROM debian:buster-slim

RUN apt-get update && apt-get install -q -y curl jq

RUN mkdir /usr/files_to_upload
COPY file_uploader/upload_files.sh /bin/upload_files
RUN chmod +x /bin/upload_files

CMD ["bash"]