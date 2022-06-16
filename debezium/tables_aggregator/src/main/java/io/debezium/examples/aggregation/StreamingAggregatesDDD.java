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

import io.debezium.examples.aggregation.model.Address;
import io.debezium.examples.aggregation.model.Addresses;
import io.debezium.examples.aggregation.model.Customer;
import io.debezium.examples.aggregation.model.CustomerAddressAggregate;
import io.debezium.examples.aggregation.model.DefaultId;
import io.debezium.examples.aggregation.model.EventType;
import io.debezium.examples.aggregation.model.LatestAddress;
import io.debezium.examples.aggregation.serdes.SerdeFactory;

public class StreamingAggregatesDDD {

    public static void main(String[] args) {

        if(args.length != 3) {
            System.err.println("usage: java -jar <package> "
                    + StreamingAggregatesDDD.class.getName() + " <parent_topic> <children_topic> <bootstrap_servers>");
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
        final Serde<CrossReference> crossReferenceSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(CrossReference.class,false);
        final Serde<LatestCrossReference> latestCrossReferenceSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(LatestCrossReference.class,false);
        final Serde<CrossReferences> crossReferencesSerde = SerdeFactory.createDbzEventJsonPojoSerdeFor(CrossReferences.class,false);
        final Serde<ReferenceCrossReferenceAggregate> aggregateSerde =
                SerdeFactory.createDbzEventJsonPojoSerdeFor(ReferenceCrossReferenceAggregate.class,false);

        StreamsBuilder builder = new StreamsBuilder();

        //1) read parent topic i.e. reference as ktable
        KTable<DefaultId, Reference> referenceTable =
                builder.table(parentTopic, Consumed.with(defaultIdSerde,referenceSerde));

        //2) read children topic i.e. cross_reference as kstream
        KStream<DefaultId, CrossReference> crossReferenceStream = builder.stream(childrenTopic,
                Consumed.with(defaultIdSerde, crossReferenceSerde));

        //2a) pseudo-aggreate addresses to keep latest relationship info
        KTable<DefaultId,LatestCrossReference> tempTable = crossReferenceStream
                .groupByKey(Serialized.with(defaultIdSerde, crossReferenceSerde))
                .aggregate(
                        () -> new LatestCrossReference(),
                        (DefaultId crossReferenceId, CrossReference cross_reference, LatestCrossReference latest) -> {
                            latest.update(cross_reference,crossReferenceId,new DefaultId(cross_reference.getReference_id()));
                            return latest;
                        },
                        Materialized.<DefaultId,LatestCrossReference,KeyValueStore<Bytes, byte[]>>
                                        as(childrenTopic+"_table_temp")
                                            .withKeySerde(defaultIdSerde)
                                                .withValueSerde(latestCrossReferenceSerde)
                );

        //2b) aggregate addresses per customer id
        KTable<DefaultId, CrossReferences> crossReferenceTable = tempTable.toStream()
                .map((addressId, latestCrossReference) -> new KeyValue<>(latestCrossReference.getReferenceId(),latestCrossReference))
                .groupByKey(Serialized.with(defaultIdSerde,latestCrossReferenceSerde))
                .aggregate(
                        () -> new CrossReferences(),
                        (referenceId, latestCrossReference, crossReferences) -> {
                            crossReferences.update(latestCrossReference);
                            return crossReferences;
                        },
                        Materialized.<DefaultId,CrossReferences,KeyValueStore<Bytes, byte[]>>
                                        as(childrenTopic+"_table_aggregate")
                                            .withKeySerde(defaultIdSerde)
                                                .withValueSerde(crossReferencesSerde)
                );

        //3) KTable-KTable JOIN to combine customer and addresses
        KTable<DefaultId,ReferenceCrossReferenceAggregate> dddAggregate =
                referenceTable.join(crossReferenceTable, (reference, crossReferences) ->
                    reference.get_eventType() == EventType.DELETE ?
                            null : new ReferenceCrossReferenceAggregate(reference,crossReferences.getEntries())
                );

        dddAggregate.toStream().to("final_ddd_aggregates",
                                    Produced.with(defaultIdSerde,(Serde)aggregateSerde));

        dddAggregate.toStream().print(Printed.toSysOut());

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
