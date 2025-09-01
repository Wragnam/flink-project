package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.flatmaps.paragon.ParagonEventEnricherFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor class responsible for consuming raw Paragon events,
 * enriching them using a flatMap transformation, and writing enriched
 * events back to Kafka.
 */
public class ParagonEventEnricher {

    /**
     * Default constructor for {@code ParagonEventEnricher}.
     */
    public ParagonEventEnricher() {}

    /**
     * Processes raw Paragon events by:
     * <ul>
     *   <li>Reading {@link ParagonRawData} from a Kafka source.</li>
     *   <li>Keying events by their serial number.</li>
     *   <li>Applying a flatMap function {@link ParagonEventEnricherFlatmap} to enrich the events.</li>
     *   <li>Writing the enriched events to a Kafka sink.</li>
     * </ul>
     *
     * @param config the configuration containing Kafka topics, Flink job name, and other settings
     * @throws Exception if the Flink job execution fails
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for ParagonRawData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ParagonRawData
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ParagonRawData> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ParagonRawData> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw Paragon events
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "paragon:data-raw->data-enriched")
                    .keyBy((KeySelector<ParagonRawData, String>) ParagonRawData::getSerial)
                    // Enrich events using ParagonEventEnricherFlatmap
                    .flatMap(new ParagonEventEnricherFlatmap(config))
                    // Write the enriched event to Kafka sink
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
