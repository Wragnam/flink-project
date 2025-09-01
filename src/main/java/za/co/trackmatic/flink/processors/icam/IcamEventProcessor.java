package za.co.trackmatic.flink.processors.icam;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.flatmaps.multipletps.UniversalLiveposEventProcessor;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.RawLiveposData;
import za.co.trackmatic.flink.models.MultipleProviders.TripAndEventMappers;
import za.co.trackmatic.flink.models.icam.IcamPositionRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor for ICAM raw position events.
 * <p>
 * Reads raw ICAM position data from Kafka, transforms it into internal RawLiveposData,
 * enriches and processes it with a universal live position processor,
 * and writes GeneralEvent results back to Kafka.
 * </p>
 */
public class IcamEventProcessor {

    /**
     * Default constructor.
     */
    public IcamEventProcessor() {
    }

    /**
     * Starts the Flink processing job.
     * <p>
     * Reads raw ICAM position data from the configured Kafka source,
     * maps the data into a common RawLiveposData format,
     * keys the stream by organization ID, applies universal live position processing,
     * and sinks processed events to the configured Kafka sink.
     * </p>
     *
     * @param config Flink and Kafka configuration parameters
     * @throws Exception if the Flink job fails to execute
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<IcamPositionRaw> deserializer = new GenericDeserializer<>(IcamPositionRaw.class);
            KafkaSource<IcamPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for enriched position data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "icam:raw->event")
                    .map(TripAndEventMappers::fromIcamForEvent)
                    .keyBy((KeySelector<RawLiveposData, String>) rawData -> rawData.getMetadata().getOrgId())
                    .flatMap(new UniversalLiveposEventProcessor(config)).sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
