package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CrossReferences {

    @JsonProperty
    @JsonSerialize(keyUsing = DefaultId.IdSerializer.class)
    @JsonDeserialize(keyUsing = DefaultId.IdDeserializer.class)
    private Map<DefaultId,Author> entries = new LinkedHashMap<>();

    public void update(LatestCrossReference cross_reference) {
        if(cross_reference.getLatest() != null) {
            entries.put(cross_reference.getCurie(),cross_reference.getLatest());
        } else {
            entries.remove(cross_reference.getAuthorId());
        }
    }

    @JsonIgnore
    public List<CrossReference> getEntries() {
        return new ArrayList<>(entries.values());
    }

    @Override
    public String toString() {
        return "CrossReferences{" +
            "entries=" + entries +
            '}';
    }
}
