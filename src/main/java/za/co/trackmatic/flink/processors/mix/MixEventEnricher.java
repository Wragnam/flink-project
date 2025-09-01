package za.co.trackmatic.flink.processors.mix;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.mix.MixEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericSerializer;
import za.co.trackmatic.flink.serde.GenericListDeserializer;

import java.io.Serializable;
import java.util.Objects;

/**
 * A Flink processor that consumes raw Mix telematics events from a Kafka topic,
 * enriches them with Trackmatic metadata, filters them based on event category,
 * and publishes the enriched events to an output Kafka topic.
 *
 * <p>This processor handles only events of category {@code "Notify"} and ensures
 * each event has a valid {@code formattedEvent} and associated metadata before sending it onward.</p>
 */
public class MixEventEnricher implements Serializable {

    /**
     * Default constructor for {@code MixEventEnricher}.
     */
    public MixEventEnricher() {
    }

    /**
     * Entry point for the Flink enrichment job.
     *
     * <p>This method configures the Flink job using the provided {@link Config}, sets up
     * Kafka source and sink, applies a transformation to enrich each {@link MixEventRaw}
     * with metadata and a formatted event name, and filters out invalid or non-relevant events.</p>
     *
     * @param config the application configuration object containing Kafka, Flink, and metadata settings
     * @throws Exception if an error occurs during the setup or execution of the Flink job
     */
    public void process(Config config) throws Exception {
        // Set up the Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Configuring the Kafka source and deserializer for MixEventRaw events
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<MixEventRaw> deserializer = new GenericListDeserializer<>(MixEventRaw.class);
            KafkaSource<MixEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configuring the Kafka sink and serializer to send the enriched data to Kafka
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<MixEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<MixEventRaw> output = Utils.createKafkaSink(source, serializer);

            // Create a data stream from the Kafka source and apply a FlatMap transformation

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "mix:event-raw->event-enriched").flatMap(new FlatMapFunction<MixEventRaw, MixEventRaw>() {
                        /**
                         * The flatMap function that processes each `MixEventRaw` record, filters the events with the category "Notify",
                         * retrieves metadata for each event, and enriches the event with the metadata.
                         *
                         * @param mixEventRaw The raw mix event to be processed
                         * @param collector The collector that emits the enriched event downstream
                         * @throws Exception If any error occurs during the processing of the event
                         */
                        @Override
                        public void flatMap(MixEventRaw mixEventRaw, Collector<MixEventRaw> collector) throws Exception {
                            // Process only events with the category "Notify"
                            if (!Objects.equals(mixEventRaw.getEventCategory(), "Notify")) {
                                return;
                            }

                            String formattedEvent = Utils.getFormattedEvent(mixEventRaw.getEventType(), SourceMapping.MIX);
                            if (formattedEvent == null) {
                                return;
                            }

                            // Clean the serial number of the asset and retrieve metadata
                            String serial = Utils.cleanSerial(String.valueOf(mixEventRaw.getAssetId()));
                            TmMetadata metadata = Utils.getMetaData(serial, config, "mix-ev-enricher");
                            if (metadata == null) {
                                return;
                            }

                            // Set the retrieved metadata to the event data
                            mixEventRaw.setMetadata(metadata);

                            mixEventRaw.setFormattedEvent(formattedEvent);

                            // Collect the enriched event to the output stream
                            collector.collect(mixEventRaw);
                        }
                    })
                    // Write the enriched data to Kafka sink
                    .sinkTo(output);
            // Execute the Flink job with the given job name from config
            env.execute(config.getFlinkJobName());
        }
    }
}

