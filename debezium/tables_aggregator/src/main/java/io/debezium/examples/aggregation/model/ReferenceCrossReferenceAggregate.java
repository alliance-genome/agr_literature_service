package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class ReferenceCrossReferenceAggregate {

    private final Reference reference;

    private final List<CrossReference> authors;

    @JsonCreator
    public ReferenceCrossReferenceAggregate(
            @JsonProperty("reference") Reference reference,
            @JsonProperty("cross_reference") List<CrossReference> cross_references) {
        this.reference = reference;
        this.cross_references = cross_references;
    }

    public Reference getReference() {
        return reference;
    }

    public List<CrossReference> getCrossReferences() {
        return cross_references;
    }

    @Override
    public String toString() {
        return "ReferenceCrossReferenceAggregate{" +
                "reference=" + reference +
                ", cross_references=" + cross_references +
                '}';
    }

}
