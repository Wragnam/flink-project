package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.flatmaps.paragon.ParagonLiveposProcessorFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor class for enriching raw Paragon live position data.
 * <p>
 * Reads raw Paragon position events from Kafka, enriches them with geofence and status information,
 * and outputs enriched live position events to Kafka.
 * </p>
 */
public class ParagonLiveposProcessor {

    /**
     * Default constructor.
     */
    public ParagonLiveposProcessor(){}

    /**
     * Sets up and executes a Flink job that:
     * <ul>
     *   <li>Reads {@link ParagonRawData} from Kafka source.</li>
     *   <li>Partitions stream by organization ID.</li>
     *   <li>Processes and enriches each event using {@link ParagonLiveposProcessorFlatmap}.</li>
     *   <li>Writes enriched {@link GeneralLocation} events back to Kafka sink.</li>
     * </ul>
     *
     * @param config Flink job and Kafka source/sink configuration.
     * @throws Exception if the Flink job execution fails.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);
            // Set the parallelism for the job
            env.setParallelism(4);

            // Configure Kafka source and deserializer for ParagonRawData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ParagonLiveposEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Read raw data from Kafka, process it, and write the enriched data back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "pg:livepos-enriched->livepos")
                    .keyBy((KeySelector<ParagonRawData, String>) rawData -> rawData.getMetadata().getOrgId())
                            .flatMap(new ParagonLiveposProcessorFlatmap(config))
                                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
