package za.co.trackmatic.flink.processors.icam;

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
import za.co.trackmatic.flink.models.icam.IcamPositionRaw;
import za.co.trackmatic.flink.models.trips.TripTopicResponse;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor for handling iCam raw position data to generate trip information.
 * <p>
 * Reads raw iCam position data from Kafka, converts it into internal RawTripData format,
 * applies trip processing logic, and writes the processed trip data back to Kafka.
 * </p>
 */
public class IcamTripProcessor {

    /**
     * Default constructor.
     */
    public IcamTripProcessor(){}

    /**
     * Main method to process raw iCam position data, perform trip-related transformations,
     * and write the processed trip data to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<IcamPositionRaw> deserializer = new GenericDeserializer<>(IcamPositionRaw.class);
            KafkaSource<IcamPositionRaw> input = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for live position events
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TripTopicResponse> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TripTopicResponse> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw position data
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "icam:livepos-enriched->trip")
                    .map(raw -> TripAndEventMappers.fromIcam(raw).build())
                    .keyBy((KeySelector<RawTripData, String>) rawData -> rawData.getMetadata().getOrgId())
                    .flatMap(new UniversalTripProcessorFlatmap(config))
                    .sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
