package za.co.trackmatic.flink.processors.scania;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericSerializer;
import za.co.trackmatic.flink.serde.GenericListDeserializer;

import java.io.Serializable;

/**
 * Flink processor for Scania live position data enrichment.
 *
 * This processor reads raw {@link ScaniaPositionRaw} events from Kafka,
 * enriches each event with metadata based on the VIN (vehicle identification number),
 * and writes the enriched events back to Kafka.
 */
public class ScaniaLiveposEnricher implements Serializable {

    /** Default constructor for ScaniaLiveposEnricher. */
    public ScaniaLiveposEnricher() {
    }

    /**
     * Main processing method which:
     * <ul>
     *   <li>Creates a Flink execution environment.</li>
     *   <li>Configures Kafka source and sink for ScaniaPositionRaw data.</li>
     *   <li>Processes the raw events to enrich them with metadata.</li>
     *   <li>Writes the enriched events to the configured Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration containing Kafka source and sink information and other job parameters.
     * @throws Exception If any error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Configure Kafka source and deserializer for ScaniaPositionRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<ScaniaPositionRaw> deserializer = new GenericListDeserializer<>(ScaniaPositionRaw.class);
            KafkaSource<ScaniaPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ScaniaPositionRaw data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ScaniaPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ScaniaPositionRaw> output = Utils.createKafkaSink(source, serializer);

            // Process the raw ScaniaPositionRaw data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "scania:livepos-raw->livepos-enriched").flatMap(new FlatMapFunction<ScaniaPositionRaw, ScaniaPositionRaw>() {
                /**
                 * Enriches each {@link ScaniaPositionRaw} event by:
                 * <ul>
                 *   <li>Cleaning the VIN to a standard serial format.</li>
                 *   <li>Retrieving associated {@link TmMetadata} from configuration cache.</li>
                 *   <li>Skipping the event if no metadata is found.</li>
                 *   <li>Setting the metadata on the event and emitting it downstream.</li>
                 * </ul>
                 *
                 * @param scaniaPositionRaw The raw Scania position event to enrich.
                 * @param collector The collector to emit the enriched event.
                 * @throws Exception If an error occurs during enrichment.
                 */
                @Override
                public void flatMap(ScaniaPositionRaw scaniaPositionRaw, Collector<ScaniaPositionRaw> collector) throws Exception {
                    // Clean the assetId and retrieve metadata associated with it
                    String serial = Utils.cleanSerial(String.valueOf(scaniaPositionRaw.getVin()));
                    scaniaPositionRaw.setVin(serial);
                    TmMetadata metadata = Utils.getMetaData(serial, config, "scania-livepos-enricher");

                    // If no metadata is found, skip the event
                    if (metadata == null) {
                        return;
                    }

                    // Set the metadata to the raw event
                    scaniaPositionRaw.setMetadata(metadata);

                    // Emit the enriched event downstream
                    collector.collect(scaniaPositionRaw);
                }
            }).sinkTo(output);// Write the enriched event to Kafka sink

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}

