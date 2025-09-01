package za.co.trackmatic.flink.processors.scania;

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
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processor for Scania live position events.
 *
 * This processor reads raw {@link ScaniaPositionRaw} data from Kafka,
 * transforms it into {@link RawLiveposData} events suitable for processing,
 * enriches and processes these events using the universal live position event processor,
 * and writes the resulting {@link GeneralEvent} objects back to Kafka.
 */
public class ScaniaLiveposEventProcessor {

    /** Default constructor for ScaniaLiveposEventProcessor. */
    public ScaniaLiveposEventProcessor() {
    }

    /**
     * Processes raw Scania position data from Kafka, transforms and enriches it,
     * and writes the resulting live position events to a Kafka sink.
     *
     * The processing steps include:
     * <ul>
     *   <li>Reading raw data from the configured Kafka source.</li>
     *   <li>Mapping {@link ScaniaPositionRaw} to {@link RawLiveposData} for processing.</li>
     *   <li>Partitioning the stream by organization ID.</li>
     *   <li>Enriching and processing events with {@link UniversalLiveposEventProcessor}.</li>
     *   <li>Writing processed events as {@link GeneralEvent} to the configured Kafka sink.</li>
     * </ul>
     *
     * @param config Configuration containing Kafka source and sink parameters, and job settings.
     * @throws Exception If any error occurs during Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ScaniaPositionRaw> deserializer = new GenericDeserializer<>(ScaniaPositionRaw.class);
            KafkaSource<ScaniaPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for enriched position data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "scania:raw->event")
                    .map(TripAndEventMappers::fromScaniaForEvent)
                    .keyBy((KeySelector<RawLiveposData, String>) rawData -> rawData.getMetadata().getOrgId())
                    .flatMap(new UniversalLiveposEventProcessor(config)).sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }

    }
}
