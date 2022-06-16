package io.debezium.examples.aggregation;

import java.util.Properties;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.state.KeyValueStore;

import io.debezium.examples.aggregation.model.Author;
import io.debezium.examples.aggregation.model.Authors;
import io.debezium.examples.aggregation.model.LatestAuthor;
import io.debezium.examples.aggregation.model.Reference;
import io.debezium.examples.aggregation.model.ReferenceAuthorAggregate;
import io.debezium.examples.aggregation.model.DefaultId;
import io.debezium.examples.aggregation.model.EventType;
import io.debezium.examples.aggregation.serdes.SerdeFactory;

public class StreamingAggregatesDDDLit {

    public static void main(String[] args) {

        if(args.length != 3) {
            System.err.println("usage: java -jar <package> "
                    + StreamingAggregatesDDDLit.class.getName() + " <parent_topic> <children_topic> <bootstrap_servers>");
            System.exit(-1);
        }

        final String parentTopic = args[0];
        final String childrenTopic = args[1];
        final String bootstrapServers = args[2];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streaming-aggregates-ddd");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10*1024);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(CommonClientConfigs.METADATA_MAX_AGE_CONFIG, 500);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Serde<DefaultId> defaultIdSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(DefaultId.class,true);
        final Serde<Reference> referenceSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Reference.class,false);
        final Serde<Author> authorSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Author.class,false);
	    final Serde<LatestAuthor> latestAuthorSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(LatestAuthor.class,false);
        final Serde<Authors> authorsSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(Authors.class,false);
        final Serde<ReferenceAuthorAggregate> aggregateSerde =
                SerdeFactory.createDbzEventJsonPojoSerdeFor(ReferenceAuthorAggregate.class,false);

        StreamsBuilder builder = new StreamsBuilder();

        //1) read parent topic i.e. references as ktable
        KTable<DefaultId, Reference> referenceTable =
                builder.table(parentTopic, Consumed.with(defaultIdSerde,referenceSerde));

        //2) read children topic i.e. authors as kstream
        KStream<DefaultId, Author> authorStream = builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, authorSerde));

	    //2a) pseudo-aggreate authors to keep latest relationship info
        KTable<DefaultId,LatestAuthor> tempTable = authorStream
                .groupByKey(Serialized.with(defaultIdSerde, authorSerde))
                .aggregate(
                        () -> new LatestAuthor(),
                        (DefaultId authorId, Author author, LatestAuthor latest) -> {
                            latest.update(author,authorId,new DefaultId(author.getReference_id()));
                            return latest;
                        },
                        Materialized.<DefaultId,LatestAuthor,KeyValueStore<Bytes, byte[]>>
                                        as(childrenTopic+"_table_temp")
                                            .withKeySerde(defaultIdSerde)
                                                .withValueSerde(latestAuthorSerde)
                );

	    //2b) aggregate authors per reference id
        KTable<DefaultId, Authors> authorTable = tempTable.toStream()
                .map((authorId, latestAuthor) -> new KeyValue<>(latestAuthor.getReferenceId(),latestAuthor))
                .groupByKey(Serialized.with(defaultIdSerde,latestAuthorSerde))
                .aggregate(
                        () -> new Authors(),
                        (referenceId, latestAuthor, authors) -> {
                            authors.update(latestAuthor);
                            return authors;
                        },
                        Materialized.<DefaultId,Authors,KeyValueStore<Bytes, byte[]>>
                                        as(childrenTopic+"_table_aggregate")
                                            .withKeySerde(defaultIdSerde)
                                                .withValueSerde(authorsSerde)
                );

        //3) KTable-KTable JOIN to combine reference and authors
        KTable<DefaultId,ReferenceAuthorAggregate> dddAggregate =
                referenceTable.join(authorTable, (reference, authors) ->
                    reference.get_eventType() == EventType.DELETE ?
                            null : new ReferenceAuthorAggregate(reference,authors.getEntries())
                );

        dddAggregate.toStream().to("references",
                                    Produced.with(defaultIdSerde,(Serde)aggregateSerde));

        dddAggregate.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
