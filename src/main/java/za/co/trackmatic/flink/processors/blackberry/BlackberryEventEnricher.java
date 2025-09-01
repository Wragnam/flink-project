package za.co.trackmatic.flink.processors.blackberry;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.blackberry.BlackberryEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches Blackberry event data by adding metadata and sends the enriched events to Kafka.
 * <p>
 * This class processes raw Blackberry events by retrieving relevant metadata for each event
 * and appending it to the event before forwarding the enriched event to the Kafka sink.
 */
public class BlackberryEventEnricher {

    /**
     * Constructs a new BlackberryEventEnricher instance.
     */
    public BlackberryEventEnricher() {

    }

    /**
     * Processes the raw Blackberry events, enriches them with metadata, and sends them to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            // Set up stream options like checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<BlackberryEventRaw> deserializer = new GenericDeserializer<>(BlackberryEventRaw.class);
            KafkaSource<BlackberryEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<BlackberryEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<BlackberryEventRaw> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich events, and write back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "bb:event-raw->event-enriched").flatMap(new FlatMapFunction<BlackberryEventRaw, BlackberryEventRaw>() {

                        /**
                         * Enriches the raw Blackberry event with metadata and forwards it.
                         *
                         * @param rawEventData the raw Blackberry event to enrich
                         * @param collector the collector to output the enriched event
                         * @throws Exception if any error occurs during event enrichment
                         */
                        @Override
                        public void flatMap(BlackberryEventRaw rawEventData, Collector<BlackberryEventRaw> collector) throws Exception {
                            // Format the event type and check if it is valid
                            String formattedEvent = Utils.getFormattedEvent(rawEventData.getType().toUpperCase(), SourceMapping.BLACKBERRYRADAR);

                            // If the formatted event type is empty, skip processing
                            if (formattedEvent == null) {
                                return;
                            }

                            // Retrieve metadata for the event's device ID
                            TmMetadata meta = Utils.getMetaData(rawEventData.getDeviceId(), config, "bb-ev-enricher");

                            // If metadata is not found, skip processing for this event
                            if (meta == null) {
                                return;
                            }

                            // Set the metadata on the raw event
                            rawEventData.setTmMetadata(meta);

                            rawEventData.setFormattedEvent(formattedEvent);

                            // Collect the enriched event for further processing
                            collector.collect(rawEventData);
                        }
                    })
                    // Sink the enriched event to Kafka
                    .sinkTo(output);

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
