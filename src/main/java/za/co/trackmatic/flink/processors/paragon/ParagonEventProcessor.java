package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.flatmaps.paragon.ParagonEventProcessorFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor class responsible for consuming raw Paragon event data,
 * enriching or transforming it using a flatMap function,
 * and outputting enriched GeneralEvent data to Kafka.
 */
public class ParagonEventProcessor {

    /**
     * Default constructor for {@code ParagonEventProcessor}.
     */
    public ParagonEventProcessor() {

    }

    /**
     * Processes raw Paragon events by:
     * <ul>
     *   <li>Reading {@link ParagonRawData} from Kafka source.</li>
     *   <li>Partitioning the stream by organization ID.</li>
     *   <li>Applying a flatMap transformation {@link ParagonEventProcessorFlatmap} for enrichment or processing.</li>
     *   <li>Writing enriched {@link GeneralEvent} data to Kafka sink.</li>
     * </ul>
     *
     * @param config the configuration object containing Kafka topics, Flink job name, and other necessary settings
     * @throws Exception if the Flink job execution fails
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 120000, 1000);

            // Configure Kafka source and deserializer for ParagonRawData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ParagonGeneralEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Create a stream from the raw input source and apply a keyBy operation based on orgId
            DataStream<ParagonRawData> inputStream = env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "paragon:data-enriched->event")
                    .keyBy((KeySelector<ParagonRawData, String>) rawData -> rawData.getMetadata().getOrgId());

            // Apply the ParagonEventProcessorFlatmap transformation to enrich or process the data
            DataStream<GeneralEvent> flatMappedStream = inputStream.flatMap(new ParagonEventProcessorFlatmap(config));

            // Write the processed events to the Kafka sink
            flatMappedStream.sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
