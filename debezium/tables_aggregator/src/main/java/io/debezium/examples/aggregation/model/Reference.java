package io.debezium.examples.aggregation.model;

import com.fasterxml.jackson.annotation.*;
import scala.Int;

import javax.sound.midi.MidiDevice;
import java.util.Date;

public class Reference {

    private final EventType _eventType;

    private final Integer reference_id;
    private final String curie;
    private final Integer merged_into_id;
    private final Integer resource_id;
    private final String title;
    private final String language;
    private final String date_published;
    private final String date_arrived_in_pubmed;
    private final String date_last_modified_in_pubmed;
    private final String volume;
    private final String plain_language_abstract;
    private final String[] pubmed_abstract_languages;
    private final String page_range;
    private final String _abstract;
    private final String[] keywords;
    private final String[] pubmed_types;
    private final String publisher;
    private final String category;
    private final String pubmed_publication_status;
    private final String issue_name;
    private final Date date_updated;
    private final Date date_created;
    private final String open_access;

    @JsonCreator
    public Reference(
            @JsonProperty("_eventType") EventType _eventType,
            @JsonProperty("reference_id") Integer reference_id,
            @JsonProperty("curie") String curie,
            @JsonProperty("merged_into_id") Integer merged_into_id,
            @JsonProperty("resource_id") Integer resource_id,
            @JsonProperty("title") String title,
            @JsonProperty("language") String language,
            @JsonProperty("date_published") String date_published,
            @JsonProperty("date_arrived_in_pubmed") String date_arrived_in_pubmed,
            @JsonProperty("date_last_modified_in_pubmed") String date_last_modified_in_pubmed,
            @JsonProperty("volume") String volume,
            @JsonProperty("plain_language_abstract") String plain_language_abstract,
            @JsonProperty("pubmed_abstract_languages") String[] pubmed_abstract_languages,
            @JsonProperty("page_range") String page_range,
            @JsonProperty("abstract") String _abstract,
            @JsonProperty("keywords") String[] keywords,
            @JsonProperty("pubmed_types") String[] pubmed_types,
            @JsonProperty("publisher") String publisher,
            @JsonProperty("category") String category,
            @JsonProperty("pubmed_publication_status") String pubmed_publication_status,
            @JsonProperty("issue_name") String issue_name,
            @JsonProperty("date_updated") Date date_updated,
            @JsonProperty("date_created") Date date_created,
            @JsonProperty("open_access") String open_access) {
        this._eventType = _eventType == null ? EventType.UPSERT : _eventType;
        this.reference_id = reference_id;
        this.curie = curie;
        this.merged_into_id = merged_into_id;
        this.resource_id = resource_id;
        this.title = title;
        this.language = language;
        this.date_published = date_published;
        this.date_arrived_in_pubmed = date_arrived_in_pubmed;
        this.date_last_modified_in_pubmed = date_last_modified_in_pubmed;
        this.volume = volume;
        this.plain_language_abstract = plain_language_abstract;
        this.pubmed_abstract_languages = pubmed_abstract_languages;
        this.page_range = page_range;
        this._abstract = _abstract;
        this.keywords = keywords;
        this.pubmed_types = pubmed_types;
        this.publisher = publisher;
        this.category = category;
        this.pubmed_publication_status = pubmed_publication_status;
        this.issue_name = issue_name;
        this.date_updated = date_updated;
        this.date_created = date_created;
        this.open_access = open_access;
    }

    public EventType get_eventType() {
        return _eventType;
    }

    public Integer getReference_id() {
        return reference_id;
    }

    public String getCurie() {
        return curie;
    }

    public Integer getMerged_into_id() {
        return merged_into_id;
    }

    public Integer getResource_id() {
        return resource_id;
    }

    public String getTitle() {
        return title;
    }

    public String getLanguage() {
        return language;
    }

    public String getDate_published() {
        return date_published;
    }

    public String getDate_arrived_in_pubmed() {
        return date_arrived_in_pubmed;
    }

    public String getDate_last_modified_in_pubmed() {
        return date_last_modified_in_pubmed;
    }

    public String getVolume() {
        return volume;
    }

    public String getPlain_language_abstract() {
        return plain_language_abstract;
    }

    public String[] getKeywords() {
        return keywords;
    }

    public String[] getPubmed_types() {
        return pubmed_types;
    }

    public String[] getPubmed_abstract_languages() {
        return pubmed_abstract_languages;
    }

    public String getPage_range() {
        return page_range;
    }

    public String get_abstract() {
        return _abstract;
    }

    public String getPublisher() {
        return publisher;
    }

    public String getCategory() {
        return category;
    }

    public String getPubmed_publication_status() {
        return pubmed_publication_status;
    }

    public String getIssue_name() {
        return issue_name;
    }

    public Date getDate_updated() {
        return date_updated;
    }

    public Date getDate_created() {
        return date_created;
    }

    public String getOpen_access() {
        return open_access;
    }

    @Override
    public String toString() {
        return "Reference{" +
                "_eventType='" + _eventType + '\'' +
                ", reference_id=" + reference_id +
                ", curie='" + curie + '\'' +
                ", merged_into_id='" + merged_into_id + '\'' +
                ", resource_id='" + resource_id + '\'' +
                ", title='" + title + '\'' +
                ", language='" + language + '\'' +
                ", date_published='" + date_published + '\'' +
                ", date_arrived_in_pubmed='" + date_arrived_in_pubmed + '\'' +
                ", date_last_modified_in_pubmed='" + date_last_modified_in_pubmed + '\'' +
                ", volume='" + volume + '\'' +
                ", plain_language_abstract='" + plain_language_abstract + '\'' +
                ", pubmed_abstract_languages='" + String.join(",",pubmed_abstract_languages) + '\'' +
                ", page_range='" + page_range + '\'' +
                ", abstract='" + _abstract + '\'' +
                ", keywords='" + String.join(",", keywords) + '\'' +
                ", pubmed_types='" + String.join(",", pubmed_types) + '\'' +
                ", publisher='" + publisher + '\'' +
                ", category='" + category + '\'' +
                ", pubmed_publication_status='" + pubmed_publication_status + '\'' +
                ", issue_name='" + issue_name + '\'' +
                ", date_updated='" + date_updated.getTime() + '\'' +
                ", date_created='" + date_created.getTime() + '\'' +
                ", open_access='" + open_access + '\'' +
                '}';
    }

}
