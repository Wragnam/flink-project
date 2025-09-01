package za.co.trackmatic.flink.processors.paragon;

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
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.Utils.ParagonUtils;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.util.Date;

/**
 * Flink processor class that reads raw Paragon data from Kafka, extracts temperature-related information,
 * converts voltage readings into temperature values, and emits enriched TemperatureEvent objects to a Kafka sink.
 */
public class ParagonTemperature {

    /**
     * Processes raw Paragon data events by:
     * <ul>
     *   <li>Reading ParagonRawData from a Kafka source.</li>
     *   <li>Extracting temperature sensor voltage readings and converting them to degrees Celsius.</li>
     *   <li>Extracting the GPS location and timestamp information.</li>
     *   <li>Enriching the event with metadata and configuration details.</li>
     *   <li>Creating TemperatureEvent objects and sending them to a Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration object containing Kafka source and sink details, as well as other settings.
     * @throws Exception If any error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream execution options like time intervals
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            ParagonUtils paragonUtils = new ParagonUtils();

            // Create a deserializer for ParagonRawData from Kafka source
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create a serializer for TemperatureEvent to send to Kafka sink
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureEvent> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Process the raw Paragon data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "pg:enriched->temperature")
                    .flatMap(new FlatMapFunction<ParagonRawData, TemperatureEvent>() {
                        /**
                         * Extracts temperature readings and location data from a ParagonRawData event,
                         * converts voltage sensor readings to Celsius temperatures, and emits TemperatureEvent objects.
                         *
                         * @param rawData The raw Paragon data containing sensor and GPS info.
                         * @param collector The collector used to emit TemperatureEvent objects downstream.
                         * @throws Exception If any error occurs during processing.
                         */
                        @Override
                        public void flatMap(ParagonRawData rawData, Collector<TemperatureEvent> collector) throws Exception {

                            DataItem lastLivepos = paragonUtils.getLocationDataItem(rawData);

                            if (lastLivepos == null) {
                                return;
                            }

                            String model = rawData.getSver();

                            Double temp1 = ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT1(), model);
                            Double temp2 = ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT2(), model);

                            double latitude = Utils.parseLatitude(lastLivepos.getGps().getLa());
                            double longitude = Utils.parseLongitude(lastLivepos.getGps().getLo());

                            TmMetadata metadata = rawData.getMetadata();

                            Date date = paragonUtils.getDateTimeFromParagonDate(lastLivepos.getGps().getDt());

                            TempEventParameters tempEventParameters = new TempEventParameters(0.0, null,
                                    null, temp1, null,
                                    null, temp2, null,
                                    null, latitude, longitude, date.getTime() / 1000,
                                    rawData.getSerial(), metadata, "TRACKMATIC", true, config.getDatacacheServer());

                            // Create a TemperatureEvent based on the values in rawData
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
