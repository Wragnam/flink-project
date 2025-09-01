package za.co.trackmatic.flink.processors.fleetboard;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.fleetboard.FleetboardEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw Fleetboard event data by adding metadata and formatting the event, then writes the enriched data
 * to a Kafka sink for further processing.
 * <p>
 * This class reads raw Fleetboard event data from a Kafka source, enriches the data with metadata,
 * and writes the enriched event data back to a Kafka sink.
 */
public class FleetboardEventEnricher {

    /**
     * Default constructor for the FleetboardEventEnricher class.
     */
    public FleetboardEventEnricher() {

    }

    /**
     * Processes the raw Fleetboard event data, enriches it with metadata, and sends it to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<FleetboardEventRaw> deserializer = new GenericDeserializer<>(FleetboardEventRaw.class);
            KafkaSource<FleetboardEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<FleetboardEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<FleetboardEventRaw> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich the event data, and write back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "fb:event-raw->event-enriched")
                    .flatMap(new FlatMapFunction<FleetboardEventRaw, FleetboardEventRaw>() {
                        /**
                         * Enriches the raw Fleetboard event data by adding metadata and formatting the event.
                         * If the event is valid, it is forwarded to the output stream.
                         *
                         * @param rawEventData the raw Fleetboard event data to process
                         * @param collector the collector to output the enriched FleetboardEventRaw
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(FleetboardEventRaw rawEventData, Collector<FleetboardEventRaw> collector) throws Exception {
                            // Format the event based on its type
                            String formattedEvent = Utils.getFormattedEvent(rawEventData.getEventType().toString(), "FLEETBOARD");
                            // If the event is invalid (empty format), skip further processing
                            if (formattedEvent == null) {
                                return;
                            }

                            // Retrieve metadata for the event based on the vehicle ID
                            TmMetadata meta = Utils.getMetaData(rawEventData.getVehicleId(), config, "fb-ev-enricher");
                            if (meta == null) {
                                return;
                            }

                            // Set the retrieved metadata to the raw event data
                            rawEventData.setMetaData(meta);

                            rawEventData.setFormattedEvent(formattedEvent);

                            // Collect the enriched event data for further processing
                            collector.collect(rawEventData);
                        }
                    })
                    // Sink the enriched event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
