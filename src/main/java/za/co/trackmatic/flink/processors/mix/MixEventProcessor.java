package za.co.trackmatic.flink.processors.mix;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.EventUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.events.mix.MixSpecificData;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.mix.MixEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Objects;

/**
 * Flink processor for Mix telematics events that enriches raw {@link MixEventRaw} records
 * into general events with additional metadata and geofence information.
 *
 * <p>This processor reads raw Mix event data from Kafka, parses and enriches the events,
 * sets geofence and control room information, and outputs {@link GeneralEvent} records
 * to Kafka.</p>
 */
public class MixEventProcessor implements Serializable {

    private static final String dataSource = SourceMapping.MIX;

    // Constructor for MixEventProcessor
    public MixEventProcessor() {
    }

    /**
     * Main method to process incoming mix events and enrich them.
     * It reads raw MixEvent data from Kafka, processes it to generate enriched MixGeneralEvent, and sends the enriched event to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     *
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Date parser for handling the start datetime of events
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            // Configure Kafka source and deserializer for MixEventRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<MixEventRaw> deserializer = new GenericDeserializer<>(MixEventRaw.class);
            KafkaSource<MixEventRaw> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for MixGeneralEvent data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(input, WatermarkStrategy.noWatermarks(), "mix:event-enrich->event")
                    .flatMap(new FlatMapFunction<MixEventRaw, GeneralEvent>() {
                        /**
                         * Processes each `MixEventRaw` event, validates the event's required fields,
                         * formats the event, sets various attributes, and enriches the event with control room IDs.
                         *
                         * @param mixEventRaw The raw mix event to be processed.
                         * @param collector The collector used to emit the enriched event downstream.
                         * @throws Exception If any error occurs during the processing of the event.
                         */
                        @Override
                        public void flatMap(MixEventRaw mixEventRaw, Collector<GeneralEvent> collector) throws Exception {
                            String eventType = mixEventRaw.getEventType();

                            // Validate that the event has a start position and event type
                            if (mixEventRaw.getStartPosition() == null || eventType == null || Objects.equals(eventType, "")) {
                                return;
                            }

                            Logger.log("In mix event processor", 5601);

                            double lat = mixEventRaw.getStartPosition().getLatitude();
                            double lon = mixEventRaw.getStartPosition().getLongitude();
                            double speed = mixEventRaw.getStartPosition().getSpeedKilometresPerHour();
                            double direction = mixEventRaw.getStartPosition().getHeading();
                            double altitude = mixEventRaw.getStartPosition().getAltitudeMetres();
                            String deviceId = String.valueOf(mixEventRaw.getAssetId());
                            TmMetadata metadata = mixEventRaw.getMetadata();

                            long created;


                            // Set the event's creation timestamp (either from the start datetime or start position timestamp)
                            if (mixEventRaw.getStartDateTime() != null) {
                                created = dateParser.parse(mixEventRaw.getStartDateTime()).getTime() / 1000;
                            } else {
                                created = dateParser.parse(mixEventRaw.getStartPosition().getTimestamp()).getTime() / 1000;
                            }

                            // Create a new MixGeneralEvent from the raw event
                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created, SourceMapping.MIX,
                                    metadata, deviceId, deviceId);
                            generalEvent.setSpeed(speed);
                            generalEvent.setAltitude(altitude);
                            generalEvent.setDirection(direction);

                            generalEvent.setMix(new MixSpecificData());
                            generalEvent.getMix().setEventId(String.valueOf(mixEventRaw.getEventId()));

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalEvent);

                            // Get the formatted event type (specific to the MIX category)
                            generalEvent.setType(mixEventRaw.getFormattedEvent());
                            generalEvent.setSourceType(eventType);

                            // Set control room IDs associated with the event
                            EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                            Logger.log("Collecting mix event", 5601);
                            // Emit the enriched event downstream
                            collector.collect(generalEvent);
                        }
                    })
                    // Write the enriched event to Kafka sink
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
