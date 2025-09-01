package za.co.trackmatic.flink.processors.rms;

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
import za.co.trackmatic.flink.models.rms.RmsLiveposRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor for Rms live position events.
 * <p>
 * Consumes raw Rms position data from Kafka, transforms it into RawLiveposData,
 * processes it through a universal live position event processor, and produces enriched GeneralEvent objects
 * back into Kafka.
 * </p>
 */
public class RmsLiveposEventProcessor {

    /**
     * Default constructor for RmsLiveposEventProcessor.
     */
    public RmsLiveposEventProcessor(){}

    /**
     * Sets up and runs the Flink job for processing live Rms position events.
     * <p>
     * It reads raw position data from Kafka, applies transformation and enrichment,
     * partitions by organization ID, and outputs enriched events to Kafka.
     * </p>
     *
     * @param config Configuration for Kafka source/sink and Flink job settings.
     * @throws Exception if the Flink job fails to execute.
     */
    public void process(Config config) throws Exception{
        // Initialize Flink's StreamExecutionEnvironment
        try(StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()){
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<RmsLiveposRaw> deserializer = new GenericDeserializer<>(RmsLiveposRaw.class);
            KafkaSource<RmsLiveposRaw> rawInput = Utils.createKafkaSource(source,deserializer);

            // Create Kafka sink for enriched position data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Define data processing pipeline
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "rms:raw->event")
                    .map(TripAndEventMappers::fromRmsForEvent)
                    .keyBy((KeySelector<RawLiveposData, String>) rawData -> rawData.getMetadata().getOrgId())
                    .flatMap(new UniversalLiveposEventProcessor(config)).sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }

    }
}
