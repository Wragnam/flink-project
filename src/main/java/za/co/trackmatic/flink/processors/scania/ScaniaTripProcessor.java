package za.co.trackmatic.flink.processors.scania;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.flatmaps.multipletps.UniversalTripProcessorFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.RawTripData;
import za.co.trackmatic.flink.models.MultipleProviders.TripAndEventMappers;
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.models.trips.TripTopicResponse;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processor for Scania trip data.
 * <p>
 * This class reads raw {@link ScaniaPositionRaw} events from Kafka,
 * transforms them into trip-related data,
 * processes the data through a universal trip processor,
 * and outputs {@link TripTopicResponse} events to Kafka.
 * </p>
 */
public class ScaniaTripProcessor {

    /** Default constructor for ScaniaTripProcessor. */
    public ScaniaTripProcessor() {}

    /**
     * Processes raw Scania position data, performs trip-related transformations,
     * and writes the processed trip data to Kafka.
     *
     * <p>Processing steps include:</p>
     * <ul>
     *   <li>Reading raw position data from Kafka source.</li>
     *   <li>Mapping raw position data to internal trip data format.</li>
     *   <li>Keying the stream by organization ID for partitioned processing.</li>
     *   <li>Applying the universal trip processor flatmap to generate trip events.</li>
     *   <li>Writing processed trip events to Kafka sink.</li>
     * </ul>
     *
     * @param config The Flink job configuration containing Kafka source and sink details.
     * @throws Exception If the Flink job execution encounters an error.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);
            env.setParallelism(3);

            // Configure Kafka source and deserializer for ScaniaPositionRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ScaniaPositionRaw> deserializer = new GenericDeserializer<>(ScaniaPositionRaw.class);
            KafkaSource<ScaniaPositionRaw> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for TripTopicResponse
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TripTopicResponse> serializerGeo = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TripTopicResponse> output = Utils.createKafkaSink(sink, serializerGeo);

            // Read data from the Kafka source, apply transformations, and write to Kafka sink
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "scania:data-raw->trip")
                    .map(raw -> TripAndEventMappers.fromScania(raw).build())
                    // Key the stream by the organization ID from the raw data
                    .keyBy((KeySelector<RawTripData, String>) rawData -> rawData.getMetadata().getOrgId())
                    // Apply the UniversalTripProcessorFlatmap transformation
                    .flatMap(new UniversalTripProcessorFlatmap(config))
                    // Sink the processed trip data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
