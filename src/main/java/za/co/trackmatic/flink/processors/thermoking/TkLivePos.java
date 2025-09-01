package za.co.trackmatic.flink.processors.thermoking;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.*;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeofenceData;
import za.co.trackmatic.flink.models.MultipleProviders.TempEventParameters;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureLiveposEvent;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureEvent;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.thermoking.ThermokingData;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Objects;

/**
 * Processes Thermoking data to enrich it and generate live position events for temperature monitoring.
 * This class reads raw Thermoking data, checks if the data is within geofences, processes temperature data,
 * and creates events with enriched information, then sends them to Kafka.
 */
public class TkLivePos {

    /**
     * Default constructor for TkLivePos.
     */
    public TkLivePos() {
    }

    /**
     * Main method to process Thermoking data, enrich it, and produce live position events.
     *
     * @param config The configuration object containing Kafka source and sink details, and other configurations.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set general stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source for Thermoking data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ThermokingData> deserializer = new GenericDeserializer<>(ThermokingData.class);
            KafkaSource<ThermokingData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink for temperature live position events
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureLiveposEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureLiveposEvent> eventOutput = Utils.createKafkaSink(sink, serializer);

            // Read raw data from Kafka, apply transformations (flatMap), and write to the output sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "tk:enriched->livepos")
                    .flatMap(new FlatMapFunction<ThermokingData, TemperatureLiveposEvent>() {
                        /**
                         * Processes each Thermoking data record and enriches it to create live position events.
                         *
                         * @param thermokingData The raw Thermoking data to be enriched.
                         * @param collector The collector used to emit the enriched live position events.
                         * @throws Exception If an error occurs during processing.
                         */
                        @Override
                        public void flatMap(ThermokingData thermokingData, Collector<TemperatureLiveposEvent> collector) throws Exception {
                            //Logger.log("tk: livepos: processing for " + thermokingData.getReeferSerialNumber());
                            // Create a new TemperatureLiveposEvent based on Thermoking data
                            TemperatureLiveposEvent ev = new TemperatureLiveposEvent(thermokingData);

                            TempEventParameters tempEventParameters = new TempEventParameters(thermokingData.getSetPoint1(), thermokingData.getSetPoint2(), thermokingData.getSetPoint3(),
                                    thermokingData.getDischargeAir1(), thermokingData.getDischargeAir2(), thermokingData.getDischargeAir3(), thermokingData.getReturnAir1(),
                                    thermokingData.getReturnAir2(), thermokingData.getReturnAir3(), thermokingData.getLatitude(), thermokingData.getLongitude(), Utils.getCreatedTimeFromThermokingData(thermokingData),
                                    thermokingData.getReeferSerialNumber(), thermokingData.getTmMetadata(), SourceMapping.THERMOKING, false, null);

                            // Create a TemperatureEvent based on available Thermoking data
                            TemperatureEvent tempEvent = Utils.createTemperatureEvent(tempEventParameters);

                            // If TemperatureEvent is created, set it in the live position event
                            if (tempEvent != null) {
                                ev.setTemperatureData(tempEvent.getTemperatureData());
                            }

                            // Check if the Thermoking data is within any geofences
                            String geofenceName = null;
                            String server = config.getDatacacheServer();
                            List<GeofenceItem> geofenceItems = CacheUtils.isPointInGeofences(server, thermokingData.getTmMetadata().getOrgId(), new LatLong(thermokingData.getLatitude(), thermokingData.getLongitude()), thermokingData.getTmMetadata().getSiteIds());

                            // If data is within a geofence, set geofence data
                            if (geofenceItems != null && !geofenceItems.isEmpty()) {
                                GeofenceData gd = new GeofenceData();
                                gd.setId(geofenceItems.get(0).getId());
                                gd.setName(geofenceItems.get(0).getName());
                                ev.setGeofenceData(gd);
                                geofenceName = geofenceItems.get(0).getName();
                            }

                            // Determine the moving status based on speed
                            Double speed = (double) thermokingData.getSpeed();
                            String movingStatus = Utils.getStatus(speed);
                            ev.setMovingStatus(movingStatus);
                            ev.setIgnition(Objects.equals(thermokingData.getIgnitionStatus(), "On"));

                            ev.setSpeed(speed);

                            // Set date/time for the event
                            ev.setDateTime(thermokingData.getDataDate());

                            Logger.log(String.format("tk-livepos: %s,%s,%s,%s,%s",
                                    ev.getDeviceId(),
                                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(ev.getCreated()), ZoneId.of("UTC")),
                                    geofenceName,
                                    ev.getLatitude(),
                                    ev.getLongitude()
                            ), 5555);

                            // Set the source for the event and collect it for output
                            ev.setSource("THERMOKING");
                            collector.collect(ev);
                        }
                    })
                    // Sink the enriched TemperatureLiveposEvent to Kafka
                    .sinkTo(eventOutput);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
