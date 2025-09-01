package za.co.trackmatic.flink.processors.lytx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.EventUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.lytx.LytxSpecificEventData;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.lytx.Behaviors;
import za.co.trackmatic.flink.models.lytx.LytxEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * A Flink processor that reads Lytx raw events from a Kafka topic, enriches them with metadata,
 * evaluates conditions such as behaviors and geofence rules, and publishes the resulting
 * {@link GeneralEvent} to a Kafka sink topic.
 */
public class LytxEventProcessor implements Serializable {

    private static final String dataSource = SourceMapping.LYTX;

    /**
     * Default constructor for `LytxEventProcessor`.
     */
    public LytxEventProcessor() {}

    /**
     * Processes the raw Lytx event data by enriching it, performing necessary transformations,
     * and sending it to Kafka.
     *
     * @param config the configuration containing Kafka source and sink details, as well as other settings
     * @throws Exception if any error occurs during processing or job execution
     */
    public void process(Config config) throws Exception {
        // Set up the Flink stream execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Date parsers for handling different date formats in the raw data
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SX");
            SimpleDateFormat dateParser2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            // Kafka source configuration and deserialization setup for raw Lytx event data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LytxEventRaw> deserializer = new GenericDeserializer<>(LytxEventRaw.class);
            KafkaSource<LytxEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Kafka sink configuration and serialization setup for LytxGeneralEvent data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Stream processing: transform the raw Lytx event data and filter based on behaviors

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lytx:event-enriched->event").flatMap(new FlatMapFunction<LytxEventRaw, GeneralEvent>() {
                        /**
                         * Transforms and enriches raw Lytx event data into a general event format,
                         * performs checks for behaviors, geofences, and triggers for door events.
                         *
                         * @param lytxEventRawData the raw Lytx event to be processed
                         * @param collector collects the transformed LytxGeneralEvent for further processing or output
                         * @throws Exception if an error occurs during the transformation
                         */
                        @Override
                        public void flatMap(LytxEventRaw lytxEventRawData, Collector<GeneralEvent> collector) throws Exception {
                            TmMetadata metadata = lytxEventRawData.getMetadata();
                            double lat = lytxEventRawData.getLatitude();
                            double lon = lytxEventRawData.getLongitude();
                            String deviceId = lytxEventRawData.getVehicleId();
                            String deviceSerial = lytxEventRawData.getErSerialNumber();
                            Double speed = lytxEventRawData.getSpeed();
                            double direction = lytxEventRawData.getHeading();


                            // Iterate over behaviors within the raw event
                            for (Behaviors item : lytxEventRawData.getBehaviors()) {
                                // Get the formatted event name from the behavior
                                String formattedEvent = Utils.getFormattedEvent(item.getName(), dataSource);
                                if (formattedEvent != null) {
                                    // Create a new general event object from the raw Lytx event
                                    GeneralEvent generalEvent = new GeneralEvent(lat, lon, 0, dataSource, metadata,
                                            deviceId, deviceSerial);
                                    generalEvent.setSpeed(speed);
                                    generalEvent.setDirection(direction);

                                    generalEvent.setLytx(new LytxSpecificEventData());
                                    generalEvent.getLytx().setEventId(lytxEventRawData.getEventId());

                                    // Try parsing the record date and set it in the general event
                                    try {
                                        Date date = dateParser.parse(lytxEventRawData.getRecordDateUTC());
                                        Calendar calendar = Calendar.getInstance();
                                        calendar.setTime(date);
                                        date = calendar.getTime();
                                        generalEvent.setCreated(date.getTime() / 1000);
                                    } catch (Exception e) {
                                        Date date = dateParser2.parse(lytxEventRawData.getRecordDateUTC());
                                        Calendar calendar = Calendar.getInstance();
                                        calendar.setTime(date);
                                        date = calendar.getTime();
                                        generalEvent.setCreated(date.getTime() / 1000);
                                    }

                                    // Set the event type based on the formatted event name
                                    generalEvent.setType(formattedEvent);

                                    // Special handling for "DRIVER_TAGGED" event
                                    if (formattedEvent.equals("DRIVER_TAGGED")) {
                                        // Check if the event trigger ID is 4 (Condition for when the trigger was the "Remote")
                                        if (lytxEventRawData.getEventTriggerId() == 4) {
                                            // Check if the event's location is within a geofence
                                            List<GeofenceItem> fences = CacheUtils.isPointInGeofences(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

                                            Utils.setLocationNameAndId(fences, generalEvent);
                                            // If stop IDs are available, check if the event occurs outside the location
                                            if (metadata.getStopIds() != null) {
                                                if (!metadata.getStopIds().isEmpty()) {
                                                    if (fences == null) {
                                                        generalEvent.setType("DOOR_OPENED_OUTSIDE_LOCATION");
                                                        EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                                                        collector.collect(generalEvent);
                                                    } else {
                                                        boolean isInGeofence = false;
                                                        for (GeofenceItem geofenceItem : fences) {
                                                            if (metadata.getStopIds().contains(geofenceItem.getId())) {
                                                                isInGeofence = true;
                                                                break;
                                                            }
                                                        }
                                                        if (!isInGeofence) {
                                                            generalEvent.setType("DOOR_OPENED_OUTSIDE_LOCATION");
                                                            EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                                                            collector.collect(generalEvent);
                                                        }
                                                    }
                                                }
                                            }

                                            // Set the event type to "DOOR_OPENED"
                                            generalEvent.setType("DOOR_OPENED");
                                        } else {
                                            return;
                                        }
                                    }

                                    // Set the source type and control room IDs in the general event
                                    generalEvent.setSourceType(item.getName());
                                    EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                                    // Collect the transformed general event
                                    collector.collect(generalEvent);
                                }
                            }
                        }
                    })
                    // Sink the transformed general event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the specified job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
