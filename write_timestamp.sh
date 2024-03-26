#!/bin/sh.
file_name="test_files.txt"
current_time=$(date "+%Y.%m.%d-%H.%M.%S")
echo $current_time >> $file_name
