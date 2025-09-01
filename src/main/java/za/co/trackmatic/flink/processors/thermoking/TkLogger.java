package za.co.trackmatic.flink.processors.thermoking;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.util.List;

public class TkLogger {

    public TkLogger() {}

    /**
     * Processes the Thermoking data, filters based on the provided device names,
     * and logs the filtered data to a Kafka sink.
     *
     * @param config The configuration containing Kafka source and sink details.
     * @param deviceNames A list of device serial numbers to filter the data by.
     * @throws Exception If there is an error during the execution.
     */
    public void process(Config config, List<String> deviceNames) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream execution options like time intervals
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Get the source configuration for Kafka and create a deserializer for ThermokingData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ThermokingData> deserializer = new GenericDeserializer<>(ThermokingData.class);
            KafkaSource<ThermokingData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Get the sink configuration for Kafka and create a serializer for ThermokingData
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ThermokingData> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ThermokingData> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Process the raw input stream: filter by device names and send to the sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "tk:enriched->logger")
                    .flatMap(new FlatMapFunction<ThermokingData, ThermokingData>() {
                        /**
                         * Filters Thermoking data based on a list of device serial numbers.
                         * If the serial number matches any in the provided list, the data
                         * is passed along to the next processing stage.
                         *
                         * @param thermokingData The incoming Thermoking data to be processed.
                         * @param collector The collector used to output the filtered data.
                         * @throws Exception If an error occurs during processing.
                         */
                        @Override
                        public void flatMap(ThermokingData thermokingData, Collector<ThermokingData> collector) throws Exception {
                            // Clean the serial number from the incoming Thermoking data
                            String serial = Utils.cleanSerial(thermokingData.getReeferSerialNumber());

                            // Check if the cleaned serial number matches any in the deviceNames list
                            for (String filter : deviceNames) {
                                if (serial.equals(filter)) {
                                    // If a match is found, collect the data for further processing
                                    collector.collect(thermokingData);
                                }
                            }
                        }
                    })
                    // Sink the filtered data to the Kafka output
                    .sinkTo(eventOutput);

            // Execute the Flink job with the specified job name
            env.execute(config.getFlinkJobName());
        }
    }
}
