package za.co.trackmatic.flink.processors.surfsight;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.surfsight.SurfsightEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processor class responsible for enriching raw Surfsight event data
 * by adding metadata and formatting event types before outputting to Kafka.
 */
public class SurfsightEventEnricher {

    /**
     * Default constructor for SurfsightEventEnricher.
     */
    public SurfsightEventEnricher() {
    }

    /**
     * Executes the Flink job which reads raw Surfsight event data from Kafka,
     * enriches each event with associated metadata and formatted event type,
     * and writes the enriched events back to Kafka.
     *
     * <p>The processing includes:</p>
     * <ul>
     *     <li>Consuming {@link SurfsightEventRaw} from Kafka source.</li>
     *     <li>Formatting the event type string to a consistent format.</li>
     *     <li>Fetching metadata for the event using the serial number.</li>
     *     <li>Setting metadata and formatted event on the raw event object.</li>
     *     <li>Emitting the enriched event downstream to Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration object containing Kafka source and sink details,
     *               and other Flink job parameters.
     * @throws Exception If an error occurs during Flink job execution or data processing.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for SurfsightEventRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<SurfsightEventRaw> deserializer = new GenericDeserializer<>(SurfsightEventRaw.class);
            KafkaSource<SurfsightEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for SurfsightEventRaw
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<SurfsightEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<SurfsightEventRaw> output = Utils.createKafkaSink(sink, serializer);

            // Read data from Kafka source, apply transformations (flatMap), and write the result to Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ss:event-raw->event-enriched")
                    .flatMap(new FlatMapFunction<SurfsightEventRaw, SurfsightEventRaw>() {
                        /**
                         * Enriches raw Surfsight event data by formatting the event type
                         * and attaching metadata fetched based on the event's serial number.
                         *
                         * @param rawSurfsightEventDataRaw The raw event to enrich.
                         * @param collector Collector to emit the enriched events.
                         * @throws Exception If enrichment or metadata fetching fails.
                         */
                        @Override
                        public void flatMap(SurfsightEventRaw rawSurfsightEventDataRaw, Collector<SurfsightEventRaw> collector) throws Exception {
                            // Format the event type to uppercase and check if it's empty
                            String formattedEvent = Utils.getFormattedEvent(rawSurfsightEventDataRaw.getData().getEventType().toUpperCase(), "SURFSIGHT");
                            if (formattedEvent == null) {
                                return;
                            }

                            // Fetch metadata based on the serial number of the event
                            TmMetadata meta = Utils.getMetaData(rawSurfsightEventDataRaw.getData().getSerialNumber(), config, "ss-ev-enricher");
                            if (meta == null) {
                                return;
                            }

                            // Set the fetched metadata to the raw Surfsight event data
                            rawSurfsightEventDataRaw.setMeta(meta);

                            rawSurfsightEventDataRaw.setFormattedEvent(formattedEvent);

                            // Collect the enriched Surfsight event data for further processing
                            collector.collect(rawSurfsightEventDataRaw);
                        }
                    })
                    // Sink the enriched Surfsight event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
