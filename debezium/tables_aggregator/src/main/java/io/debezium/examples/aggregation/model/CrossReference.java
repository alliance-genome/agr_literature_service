package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.*;

import java.util.Date;

public class CrossReference {

    private final EventType _eventType;

    private final String curie;
    private final boolean is_obsolete;
    private final Integer reference_id;
    private final Integer resource_id;
    private final String[] pages;

    @JsonCreator
    public CrossReference(
            @JsonProperty("_eventType") EventType _eventType,
            @JsonProperty("curie") String curie,
            @JsonProperty("is_obsolete") boolean is_obsolete,
            @JsonProperty("reference_id") Integer reference_id,
            @JsonProperty("resource_id") Integer resource_id,
            @JsonProperty("pages") String[] pages) {
        this._eventType = _eventType == null ? EventType.UPSERT : _eventType;
        this.curie = curie;
        this.is_obsolete = is_obsolete;
        this.reference_id = reference_id;
	    this.resource_id = resource_id;
        this.pages = pages;
    }

    public EventType get_eventType() {
        return _eventType;
    }

    public Integer getCurie() {
        return curie;
    }

    public getIs_obsolete() {
        return is_obsolete;
    }

    public Integer getReference_id() {
        return reference_id;
    }

    public Integer getResource_id() {
        return resource_id;
    }

    public String getPages() {
        return pages;
    }

    @Override
    public String toString() {
        return "CrossReference{" +
                "_eventType=" + _eventType +
                "curie=" + curie +
                ", is_obsolete=" + is_obsolete +
                ", reference_id=" + reference_id +
                ", resource_id='" + resource_id + '\'' +
                ", pages='" + pages + '\'' +
                "}";
    }

}
