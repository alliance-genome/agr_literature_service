rm -rf agr_literature_service/lit_processing/data_ingest/tmp
rm -rf agr_literature_service/lit_processing/tests/tmp
rm -rf tests/lit_processing/tmp
find ./ -name "*.log" | xargs rm -f
find ./ -name "*.*~" | xargs rm -f