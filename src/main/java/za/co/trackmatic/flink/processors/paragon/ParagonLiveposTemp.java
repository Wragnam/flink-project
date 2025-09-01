package za.co.trackmatic.flink.processors.paragon;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeofenceData;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.TempEventParameters;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureEvent;
import za.co.trackmatic.flink.models.MultipleProviders.TemperatureLiveposEvent;
import za.co.trackmatic.flink.models.paragon.DataItem;
import za.co.trackmatic.flink.models.paragon.ParagonRawData;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.Utils.ParagonUtils;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

/**
 * This class processes raw position data from Kafka, enriches it with geofence and temperature information,
 * and writes the enriched data back to Kafka. It uses Flink's streaming capabilities for processing.
 */
public class ParagonLiveposTemp {

    /**
     * Default constructor for the ParagonLiveposTemp class.
     */
    public ParagonLiveposTemp(){}


    /**
     * Main processing method for the Flink job. This method:
     * <ul>
     *     <li>Reads raw position data from a Kafka source.</li>
     *     <li>Filters and enriches the data with geofence and temperature information.</li>
     *     <li>Writes the enriched data to a Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration object containing Kafka and Flink job settings.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);
            // Set the parallelism for the job
            env.setParallelism(4);

            // Initialize date parser for timestamp parsing
            SimpleDateFormat outputParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            ParagonUtils paragonUtils = new ParagonUtils();
            outputParser.setTimeZone(TimeZone.getTimeZone("UTC"));

            // Configure Kafka source and deserializer for ParagonRawData
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonRawData> deserializer = new GenericDeserializer<>(ParagonRawData.class);
            KafkaSource<ParagonRawData> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ParagonLiveposEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<TemperatureLiveposEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<TemperatureLiveposEvent> output = Utils.createKafkaSink(sink, serializer);

            // Read raw data from Kafka, process it, and write the enriched data back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "pg:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<ParagonRawData, TemperatureLiveposEvent>() {
                        /**
                         * Processes raw position data to filter, enrich, and prepare it for further analysis.
                         *
                         * @param rawPositionData The raw position data from Kafka.
                         * @param collector The collector for emitting enriched events.
                         * @throws Exception If an error occurs during data processing.
                         */
                        @Override
                        public void flatMap(ParagonRawData rawPositionData, Collector<TemperatureLiveposEvent> collector) throws Exception {
                            DataItem lastLivepos = paragonUtils.getLocationDataItem(rawPositionData);
                            if(lastLivepos == null){
                                return;
                            }

                            TmMetadata metadata = rawPositionData.getMetadata();

                            String model = rawPositionData.getSver();

                            Double temp1 = ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT1(), model);
                            Double temp2 = ParagonUtils.convertVoltageToDegreesCelsius(lastLivepos.getT2(), model);

                            // Parse latitude and longitude from the GPS data
                            double latitude = Utils.parseLatitude(lastLivepos.getGps().getLa());
                            double longitude =Utils.parseLongitude(lastLivepos.getGps().getLo());

                            // Parse the timestamp from the GPS data
                            Date date = paragonUtils.getDateTimeFromParagonDate(lastLivepos.getGps().getDt());
                            long dateTime = date.getTime()/1000;

                            // Create a new ParagonLiveposEvent from the raw position data
                            TemperatureLiveposEvent generalPosition = new TemperatureLiveposEvent(rawPositionData, latitude, longitude, dateTime);

                            // Set the timestamp from the GPS data
                            generalPosition.setTimestamp(dateTime);

                            // Get geofence information by checking if the point is inside any geofence
                            String geofenceName = null;
                            String server = config.getDatacacheServer();
                            List<GeofenceItem> fences = CacheUtils.isPointInGeofences(server, metadata.getOrgId(), new LatLong(latitude, longitude), metadata.getSiteIds());


                            // If a geofence is found, set it in the event
                            if (fences != null && !fences.isEmpty()) {
                                GeofenceData gd = new GeofenceData();
                                gd.setId(fences.get(0).getId());
                                geofenceName = fences.get(0).getName();
                                gd.setName(geofenceName);
                                generalPosition.setGeofenceData(gd);
                            }

                            TempEventParameters tempEventParameters = new TempEventParameters(0.0, null, null, temp1, null, null,
                                    temp2, null, null, latitude, longitude, dateTime,
                                    rawPositionData.getSerial(), metadata, null, false, null);

                            // Create a temperature event if temperature data is available
                            TemperatureEvent tmpEvent = Utils.createTemperatureEvent(tempEventParameters);
                            if(tmpEvent != null){
                                generalPosition.setTemperatureData(tmpEvent.getTemperatureData());
                            }

                            // Determine the status based on GPS speed and set it in the event
                            String status = Utils.getStatus(lastLivepos.getGps().getSp());
                            generalPosition.setMovingStatus(status);

                            generalPosition.setDateTime(outputParser.format(date));

                            // Log the enriched event information
                            Logger.log(String.format("tk-livepos PARAGON: %s,%s,%s,%s,%s",
                                    generalPosition.getDeviceId(),
                                    ZonedDateTime.ofInstant(Instant.ofEpochMilli(generalPosition.getCreated()), ZoneId.of("UTC")),
                                    geofenceName,
                                    generalPosition.getLatitude(),
                                    generalPosition.getLongitude()
                            ), 5556);

                            generalPosition.setSource("TRACKMATIC");

                            // Collect the enriched live position event for output
                            collector.collect(generalPosition);
                        }
                    })
                    // Sink the enriched live position events to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }

}
