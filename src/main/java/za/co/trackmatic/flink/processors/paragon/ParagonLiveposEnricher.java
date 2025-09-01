package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor class for enriching raw Paragon position data.
 * <p>
 * Reads raw Paragon position data from Kafka, enriches each record with metadata,
 * and outputs the enriched data back to Kafka.
 * </p>
 */
public class ParagonLiveposEnricher {

    /**
     * Default constructor.
     */
    public ParagonLiveposEnricher() {}

    /**
     * Sets up and executes a Flink job that:
     * <ul>
     *   <li>Reads {@link ParagonRawData} from Kafka source.</li>
     *   <li>Filters out empty data entries.</li>
     *   <li>Enriches each event with metadata obtained from configuration and cleaned serial number.</li>
     *   <li>Emits enriched data to Kafka sink.</li>
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

            // Configure Kafka sink and serializer for ParagonRawData
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ParagonRawData> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ParagonRawData> output = Utils.createKafkaSink(sink, serializer);

            // Read data from the Kafka source, apply transformations, and write to Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "paragon:data-raw->data-enriched").flatMap(new FlatMapFunction<ParagonRawData, ParagonRawData>() {

                        /**
                         * Processes and enriches each raw Paragon position record by:
                         * <ul>
                         *   <li>Skipping records with empty data.</li>
                         *   <li>Cleaning the serial number.</li>
                         *   <li>Looking up metadata based on the serial and configuration.</li>
                         *   <li>Setting metadata on the event if found.</li>
                         *   <li>Emitting the enriched event downstream.</li>
                         * </ul>
                         *
                         * @param rawPositionData the raw Paragon position data event
                         * @param collector collector to emit enriched data downstream
                         * @throws Exception if processing fails
                         */
                        @Override
                        public void flatMap(ParagonRawData rawPositionData, Collector<ParagonRawData> collector) throws Exception {
                            // If the data is empty, skip this record
                            if(rawPositionData.getData().isEmpty()){
                                Logger.log("PARAGON Data for " + rawPositionData.getSerial() + " is empty", 5556);
                                return;
                            }

                            // Clean and extract the serial number
                            String serial = Utils.cleanSerial(rawPositionData.getSerial());

                            // Get metadata based on the serial and provided configuration
                            TmMetadata metadata = Utils.getMetaData(serial, config, "pg-livepos-enricher");
                            Logger.log("PARAGON ENRICHER for device: " + serial + " metadata: " + metadata, 5556);
                            if(metadata == null){
                                // If no metadata is found, skip the record
                                return;
                            }

                            // Set the enriched metadata to the raw position data
                            rawPositionData.setMetadata(metadata);

                            // Collect the enriched raw data to be written to the output
                            collector.collect(rawPositionData);
                        }
                    })
                    // Sink the enriched data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
