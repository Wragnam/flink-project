package za.co.trackmatic.flink.processors.surfsight;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.surfsight.SurfsightPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw Surfsight live position data by attaching metadata before forwarding the enriched data downstream.
 */
public class SurfsightLiveposEnricher {

    /**
     * Default constructor for SurfsightLiveposEnricher.
     */
    public SurfsightLiveposEnricher() {}

    /**
     * Runs the Flink pipeline to read raw Surfsight live position data from Kafka, enrich each event
     * with metadata, and write the enriched events back to Kafka.
     *
     * @param config Configuration object containing Kafka source and sink info and job parameters.
     * @throws Exception if Flink execution or processing fails.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for SurfsightPositionRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<SurfsightPositionRaw> deserializer = new GenericDeserializer<>(SurfsightPositionRaw.class);
            KafkaSource<SurfsightPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for SurfsightPositionRaw
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<SurfsightPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<SurfsightPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            // Read data from Kafka source, apply transformations (flatMap), and write the result to Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ss:lp-raw->lp-enriched")
                    .flatMap(new FlatMapFunction<SurfsightPositionRaw, SurfsightPositionRaw>() {
                        /**
                         * The flatMap function that enriches raw Surfsight live position data with metadata.
                         *
                         * @param rawPositionData The raw Surfsight live position data to be enriched.
                         * @param collector The collector used to emit the enriched SurfsightPositionRaw.
                         * @throws Exception If an error occurs during processing.
                         */
                        @Override
                        public void flatMap(SurfsightPositionRaw rawPositionData, Collector<SurfsightPositionRaw> collector) throws Exception {
                            // Retrieve metadata for the raw position data based on the serial number
                            TmMetadata meta = Utils.getMetaData(rawPositionData.getSerialNumber(),config,"ss-enricher");

                            // If metadata is null, skip processing this raw position data
                            if(meta == null){
                                return;
                            }

                            // Set the metadata on the raw position data
                            rawPositionData.setMeta(meta);

                            // Emit the enriched raw position data
                            collector.collect(rawPositionData);
                        }
                    })
                    // Sink the enriched SurfsightPositionRaw data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
