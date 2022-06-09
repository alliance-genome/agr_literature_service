# test speed of shell doing removal of directory and extension

# search_dir="dqm_sample"
search_dir="pubmed_xml"
# count=0
for entry in "$search_dir"/*
do
  # count = $((count+1))
  file=${entry/$search_dir\//}
  file=${file/.xml/}
  echo "$file"
done


