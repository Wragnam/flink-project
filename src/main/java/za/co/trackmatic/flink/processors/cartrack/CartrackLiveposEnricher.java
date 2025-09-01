package za.co.trackmatic.flink.processors.cartrack;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.cartrack.CartrackPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericListDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw Cartrack position data with additional metadata and writes the enriched data to a Kafka sink.
 * <p>
 * This class reads raw Cartrack position data from a Kafka source, enriches the data with metadata based on the device ID,
 * and writes the enriched position data to a Kafka sink for further processing.
 */
public class CartrackLiveposEnricher {
    /**
     * Constructs a new CartrackLiveposEnricher instance.
     */
    public CartrackLiveposEnricher(){}

    /**
     * Processes raw Cartrack position data, enriches it with metadata, and sends it to a Kafka sink.
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
            GenericListDeserializer<CartrackPositionRaw> deserializer = new GenericListDeserializer<>(CartrackPositionRaw.class);
            KafkaSource<CartrackPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<CartrackPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<CartrackPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich the position data, and write back to Kafka

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ct:livepos-raw->livepos-enriched").flatMap(new FlatMapFunction<CartrackPositionRaw, CartrackPositionRaw>() {
                        /**
                         * Enriches the raw Cartrack position data with metadata and forwards it to the output stream.
                         *
                         * @param rawPositionData the raw Cartrack position data to process
                         * @param collector the collector to output the enriched CartrackPositionRaw
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(CartrackPositionRaw rawPositionData, Collector<CartrackPositionRaw> collector) throws Exception {
                            // Retrieve metadata based on the device ID
                            TmMetadata meta = Utils.getMetaData(String.valueOf(rawPositionData.getVehicleId()),config,"ct-livepos-enricher");
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