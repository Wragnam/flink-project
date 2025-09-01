package za.co.trackmatic.flink.processors.thermoking;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.*;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Takes raw thermoking data and adds trackmatic fields like orgId, assetId, etc.
 * This is typically the starting point i.e. all other operations depend on the enriched
 * data
 */
public class TkEnricher {

    /**
     * Default constructor for TkEnricher.
     */
    public TkEnricher() {}

    /**
     * Main method to process raw Thermoking data, enrich it with Trackmatic metadata,
     * and write the enriched data to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for ThermokingData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ThermokingData> deserializer = new GenericDeserializer<>(ThermokingData.class);
            KafkaSource<ThermokingData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ThermokingData
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ThermokingData> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ThermokingData> output = Utils.createKafkaSink(sink, serializer);

            // Read data from Kafka source, apply transformations (flatMap), and write the result to Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "tk:raw->enriched")
                    .flatMap(new FlatMapFunction<ThermokingData, ThermokingData>() {
                        /**
                         * The flatMap function that enriches raw Thermoking data with Trackmatic metadata.
                         *
                         * @param thermokingData The raw Thermoking data to be enriched.
                         * @param collector The collector used to emit the enriched ThermokingData.
                         * @throws Exception If an error occurs during processing.
                         */
                        @Override
                        public void flatMap(ThermokingData thermokingData, Collector<ThermokingData> collector) throws Exception {
                            try {
                                // Retrieve Trackmatic metadata based on reefer serial number
                                TmMetadata meta = Utils.getMetaData(thermokingData.getReeferSerialNumber(),config,"tk-enricher");
                                if(meta==null){
                                    // Skip processing if metadata is not found
                                    return;
                                }

                                // Set the Trackmatic metadata to the Thermoking data
                                thermokingData.setTmMetadata(meta);

                                // Collect the enriched Thermoking data for output
                                collector.collect(thermokingData);
                            } catch (Exception e) {
                                Logger.log("Error in flatmap processing: " + e.getMessage(), 5555);
                                e.printStackTrace();
                            }
                        }
                    })
                    // Sink the enriched Thermoking data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
