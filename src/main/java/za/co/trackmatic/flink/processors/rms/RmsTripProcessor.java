package za.co.trackmatic.flink.processors.rms;

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
import za.co.trackmatic.flink.models.rms.RmsLiveposRaw;
import za.co.trackmatic.flink.models.trips.TripTopicResponse;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor for Rms trip data.
 * <p>
 * Reads raw Rms position data from Kafka, converts it into internal RawTripData,
 * processes it with a universal trip processor, and outputs trip-related events to Kafka.
 * </p>
 */
public class RmsTripProcessor {

    /**
     * Default constructor.
     */
    public RmsTripProcessor() {}

    /**
     * Main method to process raw Rms position data, perform trip-related transformations,
     * and write the processed trip data to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Configure Kafka source and deserializer for RmsLiveposRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<RmsLiveposRaw> deserializer = new GenericDeserializer<>(RmsLiveposRaw.class);
            KafkaSource<RmsLiveposRaw> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for TripTopicResponse
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TripTopicResponse> serializerGeo = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TripTopicResponse> output = Utils.createKafkaSink(sink, serializerGeo);

            // Read data from the Kafka source, apply transformations, and write to Kafka sink
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "rms:data-raw->trip")
                    .map(raw -> TripAndEventMappers.fromRms(raw).build())
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
