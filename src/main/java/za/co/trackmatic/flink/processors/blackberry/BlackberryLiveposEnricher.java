package za.co.trackmatic.flink.processors.blackberry;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.blackberry.BlackberryPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw Blackberry position data with additional metadata and writes the enriched data to a Kafka sink.
 * <p>
 * This class reads raw Blackberry position data from a Kafka source, enriches the data with metadata based on the device ID,
 * and writes the enriched position data to a Kafka sink for further processing.
 */
public class BlackberryLiveposEnricher {

    /**
     * Constructs a new BlackberryLiveposEnricher instance.
     */
    public BlackberryLiveposEnricher() {

    }

    /**
     * Processes raw Blackberry position data, enriches it with metadata, and sends it to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream options, such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<BlackberryPositionRaw> deserializer = new GenericDeserializer<>(BlackberryPositionRaw.class);
            KafkaSource<BlackberryPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<BlackberryPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<BlackberryPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich the position data, and write back to Kafka

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "bb:event-raw->event-enriched").flatMap(new FlatMapFunction<BlackberryPositionRaw, BlackberryPositionRaw>() {
                        /**
                         * Enriches the raw Blackberry position data with metadata and forwards it to the output stream.
                         *
                         * @param rawPositionData the raw Blackberry position data to process
                         * @param collector the collector to output the enriched BlackberryPositionRaw
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(BlackberryPositionRaw rawPositionData, Collector<BlackberryPositionRaw> collector) throws Exception {
                            // Retrieve metadata based on the device ID
                            TmMetadata meta = Utils.getMetaData(rawPositionData.getDeviceId(),config,"bb-livepos-enricher");
                            if(meta==null){
                                return;
                            }

                            // Set the retrieved metadata to the position data
                            rawPositionData.setMetadata(meta);

                            // Collect the enriched position data for further processing
                            collector.collect(rawPositionData);
                        }
                    })
                    // Sink the enriched position data to Kafka
                    .sinkTo(output);
            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
