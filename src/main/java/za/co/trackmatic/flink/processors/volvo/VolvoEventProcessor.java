package za.co.trackmatic.flink.processors.volvo;

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
import za.co.trackmatic.flink.models.volvo.VolvoLiveposRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processor class to handle raw Volvo live position data,
 * transform it into enriched events, and publish them to Kafka.
 */
public class VolvoEventProcessor {

    /**
     * Default constructor.
     */
    public VolvoEventProcessor() {

    }

    /**
     * Processes raw Volvo live position data by consuming it from Kafka,
     * mapping it to internal event representations, partitioning by organization ID,
     * applying event enrichment logic, and publishing the enriched events back to Kafka.
     *
     * @param config The configuration containing Kafka source and sink information and other job settings.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for VolvoLiveposRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<VolvoLiveposRaw> deserializer = new GenericDeserializer<>(VolvoLiveposRaw.class);
            KafkaSource<VolvoLiveposRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for VolvoGeneralEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "volvo:raw->event")
                    .map(TripAndEventMappers::fromVolvoForEvent)
                    .keyBy((KeySelector<RawLiveposData, String>) rawData -> rawData.getMetadata().getOrgId())
                    .flatMap(new UniversalLiveposEventProcessor(config)).sinkTo(output);
            env.execute(config.getFlinkJobName());
        }
    }
}
