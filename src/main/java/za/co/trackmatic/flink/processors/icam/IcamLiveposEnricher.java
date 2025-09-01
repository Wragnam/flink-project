package za.co.trackmatic.flink.processors.icam;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.icam.IcamPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericSerializer;
import za.co.trackmatic.flink.serde.GenericListDeserializer;

/**
 * This class processes raw ICAM position data, enriches it with metadata,
 * and outputs the enriched data to Kafka.
 */
public class IcamLiveposEnricher {
    /**
     * Default constructor for the IcamLiveposEnricher class.
     */
    public IcamLiveposEnricher(){}

    /**
     * Processes the raw ICAM position data, enriches it with metadata, and outputs
     * the enriched data to Kafka.
     *
     * @param config the configuration containing source and sink Kafka information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try(StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()){
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<IcamPositionRaw> deserializer = new GenericListDeserializer<>(IcamPositionRaw.class);
            KafkaSource<IcamPositionRaw> rawInput = Utils.createKafkaSource(source,deserializer);

            // Create Kafka sink for enriched position data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<IcamPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<IcamPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw position data
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "icam:livepos-raw->livepos-enriched")
                    .flatMap(new FlatMapFunction<IcamPositionRaw, IcamPositionRaw>() {
                        /**
                         * Enriches the raw position data by adding metadata and emitting the enriched data.
                         *
                         * @param rawPositionData the raw position data to be enriched
                         * @param collector the collector to emit the enriched position data
                         * @throws Exception if an error occurs during the enrichment process
                         */
                        @Override
                        public void flatMap(IcamPositionRaw rawPositionData, Collector<IcamPositionRaw> collector) throws Exception {
                            // Retrieve metadata based on the position ID
                            TmMetadata metadata = Utils.getMetaData(String.valueOf(rawPositionData.getId()), config, "icam-livepos-enricher");

                            // If metadata is not found, skip the current position data
                            if (metadata == null) {
                                return;
                            }

                            // Set the retrieved metadata on the position data
                            rawPositionData.setMetadata(metadata);

                            // Emit the enriched position data
                            collector.collect(rawPositionData);
                        }
                    })
                    // Output the enriched position data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
