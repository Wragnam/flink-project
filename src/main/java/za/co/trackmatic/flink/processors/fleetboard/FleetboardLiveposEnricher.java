package za.co.trackmatic.flink.processors.fleetboard;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.fleetboard.FleetboardPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw Fleetboard live position data with metadata.
 * <p>
 * This class reads raw Fleetboard live position data from a Kafka source, enriches the data with metadata
 * based on the vehicle ID, and sends the enriched position data to a Kafka sink for further processing.
 */
public class FleetboardLiveposEnricher {

    /**
     * Default constructor for the FleetboardLiveposEnricher class.
     */
    public FleetboardLiveposEnricher() {}

    /**
     * Processes the raw Fleetboard live position data, enriches it with metadata, and sends the enriched data
     * to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<FleetboardPositionRaw> deserializer = new GenericDeserializer<>(FleetboardPositionRaw.class);
            KafkaSource<FleetboardPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<FleetboardPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<FleetboardPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read raw position data from Kafka, enrich it with metadata and output the enriched data to the Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "fb:lp-raw->lp-enriched")
                    .flatMap(new FlatMapFunction<FleetboardPositionRaw, FleetboardPositionRaw>() {
                        /**
                         * Processes and enriches raw Fleetboard position data with metadata.
                         *
                         * @param rawPositionData the raw position data that needs to be enriched
                         * @param collector the collector used to emit the enriched position data
                         * @throws Exception if an error occurs during the processing
                         */
                        @Override
                        public void flatMap(FleetboardPositionRaw rawPositionData, Collector<FleetboardPositionRaw> collector) throws Exception {
                            // Enrich the raw position data with metadata based on the vehicle ID
                            TmMetadata meta = Utils.getMetaData(rawPositionData.getVehicleId(),config,"fb-livepos-enrich");
                            if(meta==null){
                                return;
                            }
                            rawPositionData.setMetaData(meta);

                            // Collect the enriched position data
                            collector.collect(rawPositionData);
                        }
                    })
                    // Output the enriched position data to Kafka
                    .sinkTo(output);

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
