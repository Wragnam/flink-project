package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.flatmaps.paragon.ParagonTripProcessorFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trips.TripTopicResponse;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor to handle raw Paragon position data,
 * process trip-related information, and output enriched trip data to Kafka.
 */
public class ParagonTripProcessor {

    /** Default constructor for ParagonTripProcessor. */
    public ParagonTripProcessor() {}

    /**
     * Sets up and executes a Flink streaming job that:
     * <ul>
     *   <li>Reads raw Paragon data from a Kafka source.</li>
     *   <li>Keys the stream by organization ID for parallel processing.</li>
     *   <li>Applies the ParagonTripProcessorFlatmap transformation to extract and process trip data.</li>
     *   <li>Writes the resulting trip events as {@link TripTopicResponse} objects to a Kafka sink.</li>
     * </ul>
     *
     * @param config Configuration object containing Kafka source and sink settings and other job parameters.
     * @throws Exception If the Flink job execution fails or encounters errors during processing.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);
            env.setParallelism(3);

            // Configure Kafka source and deserializer for ParagonRawData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for TripTopicResponse
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TripTopicResponse> serializerGeo = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TripTopicResponse> output = Utils.createKafkaSink(sink, serializerGeo);

            // Read data from the Kafka source, apply transformations, and write to Kafka sink
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "paragon:data-raw->trip")
                    // Key the stream by the organization ID from the raw data
                    .keyBy((KeySelector<ParagonRawData, String>) rawData -> rawData.getMetadata().getOrgId())
                    // Apply the ParagonTripProcessorFlatmap transformation
                    .flatMap(new ParagonTripProcessorFlatmap(config))
                    // Sink the processed trip data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
