package za.co.trackmatic.flink.processors.thermoking;

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
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.serde.GenericSerializer;
import za.co.trackmatic.flink.serde.GenericDeserializer;

/**
 * Processor for Thermoking raw data that consumes data from Kafka, transforms it into
 * a unified event format, partitions it by organization, applies event enrichment,
 * and outputs the enriched events back to Kafka.
 */
public class TkRaw {

    public TkRaw() {}

    /**
     * Processes Thermoking data from Kafka, applies transformations and enrichments,
     * and sends the resulting events to a Kafka sink.
     *
     * @param config The configuration containing Kafka source and sink details.
     * @throws Exception If there is an error during the execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream execution options like time intervals
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create a deserializer for ThermokingData from Kafka source
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ThermokingData> deserializer = new GenericDeserializer<>(ThermokingData.class);
            KafkaSource<ThermokingData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create a serializer for TpsEvent to send to Kafka sink
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Process the raw Thermoking data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "tk:enriched->event")
                    .map(TripAndEventMappers::fromThermokingForEvent)
                    // Partition the stream by Org ID
                    .keyBy((KeySelector<RawLiveposData, String>) rawLiveposData -> {
                        //always partition by org. Each partition will then have a map
                        //of device state
                        //System.out.println("Keying by: " + orgId);

                        // Key by the Org ID to partition the data by organization
                        return rawLiveposData.getMetadata().getOrgId();

                    })
                    // Apply a flatMap transformation for raw Thermoking data
                    .flatMap(new UniversalLiveposEventProcessor(config))
                    // Sink the processed events to Kafka
                    .sinkTo(eventOutput);

            // Execute the Flink job with the specified job name
            env.execute(config.getFlinkJobName());
        }
    }
}
