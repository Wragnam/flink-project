package za.co.trackmatic.flink.processors.fleetboard;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.flatmaps.fleetboard.FuelVerificationFlatmap;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.fleetboard.FuelConfirmation;
import za.co.trackmatic.flink.models.fleetboard.FuelVerificationData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processes raw Fleetboard fuel verification data, applies a custom flatMap function for verification,
 * and outputs the fuel confirmation data to a Kafka sink.
 * <p>
 * This class reads raw fuel verification data from a Kafka source, applies a fuel verification process,
 * and sends the fuel confirmation data to a Kafka sink for further processing.
 */
public class FleetboardFuelVerifierProcessor {

    /**
     * Default constructor for the FleetboardFuelVerifierProcessor class.
     */
    public FleetboardFuelVerifierProcessor(){}

    /**
     * Processes the raw Fleetboard fuel verification data, applies the fuel verification logic,
     * and sends the fuel confirmation data to a Kafka sink.
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
            GenericDeserializer<FuelVerificationData> deserializer = new GenericDeserializer<>(FuelVerificationData.class);
            KafkaSource<FuelVerificationData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<FuelConfirmation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<FuelConfirmation> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read raw fuel verification data from Kafka, apply the FuelVerificationFlatmap function and output the result to the Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "fb:fuel-raw->fuel-confirm")
                    .flatMap(new FuelVerificationFlatmap())
                    // For debugging or logging purposes, print the results to the console
                    .print();

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
