package org.alliancegenome.ksql;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * TET_GROUP_COLLECT(tag_map): collect_list for the topic_entity_tag_groups collapse (SCRUM-6630)
 * that stops carrying a group's tags once the group is large-scale.
 *
 * <p>A table aggregate re-reads, re-validates and re-writes its whole state on every input row.
 * With collect_list a 36k-tag group made every tag re-serialise a 5-19 MB list (O(n^2), ~1 tag/s on
 * the 2026-10-03 prod rebuild), and even capped at 250 every big-group tag re-wrote a 130 KB list
 * (group thread pegged on dev, 36k-tag group ~81 s locally vs ~15 s with this UDAF). This UDAF keeps the list while the group has at most THRESHOLD tags -- exactly
 * collect_list -- and when tag THRESHOLD+1 arrives it drops the list to the first tag plus an
 * overflow marker, so every later tag of that group touches a ~1 KB state. The query's own count(*)
 * stays exact, and the summary CASE (TAG_COUNT &lt;= THRESHOLD) then emits one summary tag whose
 * descriptive fields come from the kept first tag.
 *
 * <p>map() never returns the marker. Caveat: once a group has overflowed, its dropped tags cannot
 * be restored, so if deletions bring it back to THRESHOLD or fewer it shows only the kept tag until
 * the next full rebuild (adds-only bulk loads never do this).
 *
 * <p>THRESHOLD must equal LARGE_SCALE_THRESHOLD in lit_processing/data_ingest/utils/large_scale.py
 * and the ksql CASE -- tests/test_debezium_large_scale_threshold.py fails CI on drift.
 */
@UdafDescription(
    name = "tet_group_collect",
    description = "collect_list that keeps only the first element (plus an overflow marker) once more "
        + "than " + TetGroupCollectUdaf.THRESHOLD + " elements have been collected (SCRUM-6630).",
    author = "Alliance of Genome Resources")
public final class TetGroupCollectUdaf {

    public static final int THRESHOLD = 250;

    static final String OVERFLOW_KEY = "__tet_group_overflow__";

    private TetGroupCollectUdaf() {
    }

    @UdafFactory(description = "Collect tag maps, dropping all but the first once the group exceeds the threshold.")
    public static TableUdaf<Map<String, String>, List<Map<String, String>>, List<Map<String, String>>> collect() {
        return new Collect();
    }

    static boolean overflowed(final List<Map<String, String>> agg) {
        return !agg.isEmpty() && agg.get(agg.size() - 1) != null
            && agg.get(agg.size() - 1).containsKey(OVERFLOW_KEY);
    }

    static List<Map<String, String>> overflow(final List<Map<String, String>> agg) {
        final List<Map<String, String>> kept = new ArrayList<>(2);
        kept.add(agg.get(0));
        kept.add(Collections.singletonMap(OVERFLOW_KEY, "true"));
        return kept;
    }

    static final class Collect
        implements TableUdaf<Map<String, String>, List<Map<String, String>>, List<Map<String, String>>> {

        @Override
        public List<Map<String, String>> initialize() {
            return new ArrayList<>();
        }

        @Override
        public List<Map<String, String>> aggregate(final Map<String, String> tag, final List<Map<String, String>> agg) {
            if (overflowed(agg)) {
                return agg;
            }
            final List<Map<String, String>> next = new ArrayList<>(agg);
            next.add(tag);
            return next.size() > THRESHOLD ? overflow(next) : next;
        }

        @Override
        public List<Map<String, String>> undo(final Map<String, String> tag, final List<Map<String, String>> agg) {
            if (overflowed(agg)) {
                // the dropped tags are gone; count(*) still tracks the group size exactly
                return agg;
            }
            final List<Map<String, String>> next = new ArrayList<>(agg);
            next.remove(tag);
            return next;
        }

        @Override
        public List<Map<String, String>> merge(final List<Map<String, String>> left,
                                               final List<Map<String, String>> right) {
            if (overflowed(left)) {
                return left;
            }
            if (overflowed(right)) {
                return left.isEmpty() ? right : overflow(left);
            }
            final List<Map<String, String>> next = new ArrayList<>(left);
            next.addAll(right);
            return next.size() > THRESHOLD ? overflow(next) : next;
        }

        @Override
        public List<Map<String, String>> map(final List<Map<String, String>> agg) {
            return overflowed(agg) ? new ArrayList<>(agg.subList(0, 1)) : agg;
        }
    }
}
