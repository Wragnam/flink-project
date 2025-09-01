package za.co.trackmatic.flink.processors.lytx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.lytx.Behaviors;
import za.co.trackmatic.flink.models.lytx.LytxEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;

/**
 * A Flink processor that enriches raw Lytx event data with metadata and filters for meaningful events.
 * <p>
 * It consumes Lytx event messages from Kafka, evaluates their behaviors, enriches them with vehicle metadata,
 * and then writes the enriched events back to a Kafka topic.
 */
public class LytxEventEnricher implements Serializable {
    /**
     * Default constructor for `LytxEventEnricher`.
     */
    public LytxEventEnricher() {
    }

    /**
     * Processes the raw Lytx event data by enriching it with metadata and filtering based on behaviors.
     * The enriched data is then sent to Kafka.
     *
     * @param config the configuration containing Kafka source and sink details, as well as other settings
     * @throws Exception if any error occurs during processing or job execution
     */
    public void process(Config config) throws Exception {
        // Set up the Flink stream execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Kafka source configuration and deserialization setup for raw Lytx event data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LytxEventRaw> deserializer = new GenericDeserializer<>(LytxEventRaw.class);
            KafkaSource<LytxEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Kafka sink configuration and serialization setup for enriched Lytx event data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<LytxEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<LytxEventRaw> output = Utils.createKafkaSink(sink, serializer);

            // Stream processing: enrich raw Lytx event data by adding metadata and filtering based on behaviors

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lytx:event-raw->event-enriched").flatMap(new FlatMapFunction<LytxEventRaw, LytxEventRaw>() {
                        /**
                         * Transforms and enriches raw Lytx event data by adding metadata and filtering based on event behaviors.
                         *
                         * @param lytxEventRawData the raw Lytx event data to be processed
                         * @param collector collects the enriched LytxEventRaw for further processing or output
                         * @throws Exception if an error occurs during the transformation
                         */
                        @Override
                        public void flatMap(LytxEventRaw lytxEventRawData, Collector<LytxEventRaw> collector) throws Exception {
                            // Skip processing if behaviors data is null or empty
                            if (lytxEventRawData.getBehaviors() == null || lytxEventRawData.getBehaviors().isEmpty()) {
                                return;
                            }

                            // Check if any of the behaviors match a formatted event
                            int count = 0;
                            for (Behaviors item : lytxEventRawData.getBehaviors()) {
                                String formattedEvent = Utils.getFormattedEvent(item.getName(), "LYTX");
                                if (formattedEvent != null) {
                                    count += 1;
                                    break;
                                }
                            }

                            // If no matching behavior is found, skip processing
                            if (count == 0) {
                                return;
                            }

                            // Retrieve metadata for the vehicle associated with the event
                            TmMetadata meta = Utils.getMetaData(lytxEventRawData.getVehicleId(), config, "lytx-ev-enricher");
                            if (meta == null) {
                                return;
                            }

                            // Set the enriched metadata to the Lytx event
                            lytxEventRawData.setMetadata(meta);

                            // Collect the enriched event for further processing
                            collector.collect(lytxEventRawData);
                        }
                    })
                    // Sink the enriched Lytx event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the specified job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
