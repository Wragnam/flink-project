package za.co.trackmatic.flink.processors.lynx;

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
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * A Flink processor that transforms enriched {@link LynxRawData} into {@link TemperatureEvent}
 * objects and publishes them to a Kafka topic.
 *
 * <p>The transformation includes extracting location, temperature readings, and metadata
 * to construct a standardized temperature event format.
 */
public class LynxTemperatureProcessor {
    /**
     * Processes raw Lynx data by extracting relevant temperature information, transforming it into
     * a `TemperatureEvent`, and sinking the transformed data to Kafka.
     *
     * @param config the configuration for Kafka source and sink, along with other processing options
     * @throws Exception if any error occurs during processing
     */
    public void process(Config config) throws Exception {
        // Create Flink execution environment for stream processing
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic data stream options for the environment
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Kafka source configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LynxRawData> deserializer = new GenericDeserializer<>(LynxRawData.class);
            KafkaSource<LynxRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Kafka sink configuration for sinking the enriched temperature data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureEvent> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Process the raw input stream: Extract relevant temperature data and transform into TemperatureEvent
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lynx:enriched->temperature")
                    .flatMap(new FlatMapFunction<LynxRawData, TemperatureEvent>() {
                        /**
                         * Transforms raw Lynx data into a `TemperatureEvent` by extracting relevant temperature information.
                         *
                         * @param rawData the raw data to be transformed
                         * @param collector collects the transformed TemperatureEvent for further processing
                         * @throws Exception if an error occurs during the transformation
                         */
                        @Override
                        public void flatMap(LynxRawData rawData, Collector<TemperatureEvent> collector) throws Exception {

                            double lat = Double.parseDouble(rawData.getPositionInfo().getLat());
                            double lon = Double.parseDouble(rawData.getPositionInfo().getLon());
                            TmMetadata metadata = rawData.getMetadata();

                            TempEventParameters tempEventParameters = new TempEventParameters(rawData.getTemperatureInfo().getSetpointTemp1(),
                                    rawData.getTemperatureInfo().getSetpointTemp2(),rawData.getTemperatureInfo().getSetpointTemp3(),
                                    rawData.getTemperatureInfo().getSupplyAirTemp1(), rawData.getTemperatureInfo().getSupplyAirTemp2(),
                                    rawData.getTemperatureInfo().getSupplyAirTemp3(), rawData.getTemperatureInfo().getReturnAirTemp1(),
                                    rawData.getTemperatureInfo().getReturnAirTemp2(), rawData.getTemperatureInfo().getReturnAirTemp3(),
                                    lat, lon, Utils.getTimeFromString(rawData.getEventDateTimeUTC(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                                    rawData.getAssetInfo().getTruSerialNumber(), metadata, "LYNX", true, config.getDatacacheServer());

                            // Create a TemperatureEvent using the extracted temperature data and additional information
                            TemperatureEvent ev = Utils.createTemperatureEvent(tempEventParameters);

                            // If the event is null, skip it
                            if (ev == null){
                                return;
                            }

                            // Collect the transformed temperature event for sinking
                            collector.collect(ev);
                        }
                    })
                    // Sink the enriched temperature events to Kafka
                    .sinkTo(eventOutput);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}

