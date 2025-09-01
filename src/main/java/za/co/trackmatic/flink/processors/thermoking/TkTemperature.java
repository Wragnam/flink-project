package za.co.trackmatic.flink.processors.thermoking;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.MultipleProviders.TempEventParameters;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureEvent;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Thermoking temperature event processor that reads Thermoking data from Kafka,
 * extracts temperature-related events, and sends the processed temperature
 * events back to Kafka.
 */
public class TkTemperature {

    /**
     * Processes Thermoking data from Kafka, extracts temperature-related events,
     * and sends them to a Kafka sink as `TemperatureEvent` objects.
     *
     * @param config The configuration containing Kafka source and sink details.
     * @throws Exception If there is an error during the execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream execution options like time intervals
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Create a deserializer for ThermokingData from Kafka source
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ThermokingData> deserializer = new GenericDeserializer<>(ThermokingData.class);
            KafkaSource<ThermokingData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create a serializer for TemperatureEvent to send to Kafka sink
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureEvent> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Process the raw Thermoking data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "tk:enriched->temperature")
                    .flatMap(new FlatMapFunction<ThermokingData, TemperatureEvent>() {
                        /**
                         * Extracts temperature-related events from Thermoking data.
                         * This method converts the raw Thermoking data into `TemperatureEvent` objects,
                         * setting the appropriate values and including metadata.
                         *
                         * @param thermokingData The input Thermoking data to be processed.
                         * @param collector The collector used to emit the resulting events.
                         * @throws Exception If there is an error while processing the data.
                         */
                        @Override
                        public void flatMap(ThermokingData thermokingData, Collector<TemperatureEvent> collector) throws Exception {

                            double lat = thermokingData.getLatitude();
                            double lon = thermokingData.getLongitude();
                            TmMetadata metadata = thermokingData.getTmMetadata();

                            TempEventParameters tempEventParameters = new TempEventParameters(thermokingData.getSetPoint1(), thermokingData.getSetPoint2(),
                                    thermokingData.getSetPoint3(), thermokingData.getDischargeAir1(),thermokingData.getDischargeAir2(),
                                    thermokingData.getDischargeAir3(), thermokingData.getReturnAir1(), thermokingData.getReturnAir2(),
                                    thermokingData.getReturnAir3(), lat, lon, Utils.getCreatedTimeFromThermokingData(thermokingData),
                                    thermokingData.getReeferSerialNumber(), metadata, "THERMOKING", true, config.getDatacacheServer());

                            // Create a TemperatureEvent based on the values in thermokingData
                            TemperatureEvent ev = Utils.createTemperatureEvent(tempEventParameters);

                            // If the event is null, return (do not emit)
                            if (ev == null) {
                                return;
                            }

                            //Logger.log("tk: temp: ev type: " + ev.getType() + " for serial: " + ev.getDeviceId());

                            // Collect the generated TemperatureEvent
                            collector.collect(ev);
                        }
                    })
                    // Sink the processed temperature events to Kafka
                    .sinkTo(eventOutput);

            // Execute the Flink job with the specified job name
            env.execute(config.getFlinkJobName());
        }
    }
}
