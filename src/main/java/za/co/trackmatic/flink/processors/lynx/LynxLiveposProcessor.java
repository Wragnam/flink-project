package za.co.trackmatic.flink.processors.lynx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.TempEventParameters;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureEvent;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureLiveposEvent;
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;


import java.util.List;

/**
 * This class processes live position data from Lynx, enriches it with temperature and geofence information,
 * and outputs the enriched data as TemperatureLiveposEvent to Kafka.
 */
public class LynxLiveposProcessor {

    private static final String originalFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    /**
     * Default constructor for LynxLiveposProcessor.
     */
    public LynxLiveposProcessor() {}

    /**
     * Processes raw Lynx position data, enriches it with temperature and geofence data,
     * and outputs the enriched data as TemperatureLiveposEvent to Kafka.
     *
     * @param config the configuration containing Kafka source and sink information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {

            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Create Kafka source for raw Lynx data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LynxRawData> deserializer = new GenericDeserializer<>(LynxRawData.class);
            KafkaSource<LynxRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for enriched TemperatureLiveposEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureLiveposEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureLiveposEvent> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw Lynx data by applying the flatMap function
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lynx:enriched->livepos")
                    .flatMap(new FlatMapFunction<LynxRawData, TemperatureLiveposEvent>() {
                        /**
                         * Processes a raw Lynx data event and enriches it with temperature, geofence, and movement data.
                         *
                         * @param rawData the raw Lynx data to be processed
                         * @param collector collects the enriched TemperatureLiveposEvent for downstream processing
                         * @throws Exception if any error occurs during processing
                         */
                        @Override
                        public void flatMap(LynxRawData rawData, Collector<TemperatureLiveposEvent> collector) throws Exception {
                            // Create TemperatureLiveposEvent from raw Lynx data
                            TemperatureLiveposEvent ev = new TemperatureLiveposEvent(rawData);

                            TempEventParameters tempEventParameters = new TempEventParameters(rawData.getTemperatureInfo().getSetpointTemp1(),
                                    rawData.getTemperatureInfo().getSetpointTemp2(), rawData.getTemperatureInfo().getSetpointTemp3(),
                                    rawData.getTemperatureInfo().getSupplyAirTemp1(), rawData.getTemperatureInfo().getSupplyAirTemp2(),
                                    rawData.getTemperatureInfo().getSupplyAirTemp3(), rawData.getTemperatureInfo().getReturnAirTemp1(),
                                    rawData.getTemperatureInfo().getReturnAirTemp2(), rawData.getTemperatureInfo().getReturnAirTemp3(),
                                    Double.parseDouble(rawData.getPositionInfo().getLat()), Double.parseDouble(rawData.getPositionInfo().getLon()), Utils.getTimeFromString(rawData.getEventDateTimeUTC(), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
                                    rawData.getAssetInfo().getTruSerialNumber(), rawData.getMetadata(), null, false, null);

                            // Create TemperatureEvent using the raw data's temperature info
                            TemperatureEvent tev = Utils.createTemperatureEvent(tempEventParameters);
                            if (tev != null) {
                                ev.setTemperatureData(tev.getTemperatureData());
                            }

                            // Retrieve geofence information for the live position
                            String server = config.getDatacacheServer();

                            List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(server, ev.getOrgId(), new LatLong(ev.getLatitude(), ev.getLongitude()), ev.getSiteIds());

                            Utils.setLocationNameAndId(geofenceItems, ev);
                            ev.setSpeed(rawData.getStatusInfo().getPositionSpeed());

                            // Determine the moving status based on position speed
                            String movingStatus = Utils.getStatus(rawData.getStatusInfo().getPositionSpeed());
                            ev.setMovingStatus(movingStatus);

                            // Set event datetime
                            ev.setDateTime(rawData.getEventDateTimeUTC());
                            ev.setDate(Utils.convertDate(rawData.getEventDateTimeUTC(), originalFormat, Utils.dateFormat));
                            ev.setDischargeAir(rawData.getTemperatureInfo().getSupplyAirTemp1());
                            ev.setReturnAir(rawData.getTemperatureInfo().getReturnAirTemp1());

                            Logger.log(String.format("lynx-livepos: %s,%s,%s,%s",
                                    ev.getDeviceId(),
                                    ev.getCreated(),
                                    ev.getLatitude(),
                                    ev.getLongitude()
                            ), 5602);

                            // Set the source of the event
                            ev.setSource("LYNX");

                            // Collect the enriched event
                            collector.collect(ev);
                        }
                    })
                    // Sink the enriched events to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }

}
