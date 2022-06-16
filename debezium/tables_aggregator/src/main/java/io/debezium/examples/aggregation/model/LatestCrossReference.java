package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LatestCrossReference {

    private DefaultId crossReferenceId;

    private DefaultId referenceId;

    private CrossReference latest;

    public LatestCrossReference() {}

    @JsonCreator
    public LatestCrossReference(
            @JsonProperty("crossReferenceId") DefaultId crossReferenceId,
            @JsonProperty("referenceId") DefaultId referenceId,
            @JsonProperty("latest") CrossReference latest) {
        this.crossReferenceId = crossReferenceId;
        this.referenceId = referenceId;
        this.latest = latest;
    }

    public void update(CrossReference cross_reference, DefaultId crossReferenceId, DefaultId referenceId) {
        if(EventType.DELETE == cross_reference.get_eventType()) {
            latest = null;
            return;
        }
        latest = cross_reference;
        this.crossReferenceId = cross_reference;
        this.referenceId = referenceId;
    }

    public DefaultId getCrossReferenceId() {
        return crossReferenceId;
    }

    public DefaultId getReferenceId() {
        return referenceId;
    }

    public CrossReference getLatest() {
        return latest;
    }

    @Override
    public String toString() {
        return "LatestChild{" +
            "crossReferenceId=" + crossReferenceId +
            ", referenceId=" + referenceId +
            ", latest=" + latest +
            '}';
    }
}
