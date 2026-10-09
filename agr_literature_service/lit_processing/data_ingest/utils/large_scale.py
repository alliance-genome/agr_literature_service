"""The large-scale (high-throughput) association threshold (SCRUM-6614).

A (reference, entity type, topic, data provider, SEA group, owning MOD) tag
group with MORE than this many associations is summarized at search-index
time: the ksql collapse replaces the group with one synthetic summary tag
(large_scale_tag='true', entity_count=N). The loaders use the same number
for their curator-email reporting of large-scale papers.

Defined once here because the number also lives in debezium/ksql_queries.ksql
(``CASE WHEN TAG_COUNT <= N``): tests/test_debezium_large_scale_threshold.py
asserts the two stay equal, so changing either without the other fails CI
instead of producing curator reports that disagree with search.
"""

LARGE_SCALE_THRESHOLD = 250
