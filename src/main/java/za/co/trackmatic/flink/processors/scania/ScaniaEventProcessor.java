package za.co.trackmatic.flink.processors.scania;

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
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * Flink processor that consumes raw Scania position data,
 * enriches events with geofence and metadata information,
 * and outputs processed {@link GeneralEvent} to Kafka.
 */
public class ScaniaEventProcessor implements Serializable {

    private static final String dataSource = SourceMapping.SCANIA;

    /** Default constructor for ScaniaEventProcessor. */
    public ScaniaEventProcessor() {
    }

    /**
     * Main processing method to read raw Scania events from Kafka,
     * transform and enrich them, then write enriched events back to Kafka.
     *
     * The processing includes:
     * <ul>
     *   <li>Reading raw {@link ScaniaPositionRaw} data from Kafka source.</li>
     *   <li>Parsing event type and validating it.</li>
     *   <li>Extracting location, speed, direction, altitude, and timestamp information.</li>
     *   <li>Creating and enriching {@link GeneralEvent} with geofence info and control room IDs.</li>
     *   <li>Emitting the enriched events to the Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration containing Kafka source/sink details and job parameters.
     * @throws Exception If Flink job execution or event processing fails.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");


            // Configure Kafka source and deserializer for ScaniaPositionRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ScaniaPositionRaw> deserializer = new GenericDeserializer<>(ScaniaPositionRaw.class);
            KafkaSource<ScaniaPositionRaw> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for MixGeneralEvent data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(input, WatermarkStrategy.noWatermarks(), "scania:livepos-enrich->event")
                    .flatMap(new FlatMapFunction<ScaniaPositionRaw, GeneralEvent>() {
                        /**
                         * Processes each raw Scania position event by:
                         * <ul>
                         *   <li>Validating and formatting the event type.</li>
                         *   <li>Extracting positional data (latitude, longitude, speed, heading, altitude).</li>
                         *   <li>Creating a {@link GeneralEvent} enriched with geofence and control room data.</li>
                         *   <li>Emitting the enriched event downstream.</li>
                         * </ul>
                         *
                         * @param scaniaPositionRaw The incoming raw Scania position data.
                         * @param collector Collector to emit the enriched {@link GeneralEvent}.
                         * @throws Exception If parsing or enrichment fails.
                         */
                        @Override
                        public void flatMap(ScaniaPositionRaw scaniaPositionRaw, Collector<GeneralEvent> collector) throws Exception {
                            String eventType = scaniaPositionRaw.getTriggerType().getTriggerType();

                            String formattedEvent = Utils.getFormattedEvent(eventType, dataSource);
                            if (formattedEvent == null) {
                                return;
                            }

                            double lat = scaniaPositionRaw.getGnssPosition().getLatitude();
                            double lon = scaniaPositionRaw.getGnssPosition().getLongitude();
                            double speed = scaniaPositionRaw.getWheelBasedSpeed();
                            Double direction = scaniaPositionRaw.getGnssPosition().getHeading() != null ?
                                    Double.parseDouble(scaniaPositionRaw.getGnssPosition().getHeading()) : null;
                            Double altitude = scaniaPositionRaw.getGnssPosition().getAltitude() != null ?
                                    scaniaPositionRaw.getGnssPosition().getAltitude().doubleValue() : null;
                            String deviceId = String.valueOf(scaniaPositionRaw.getVin());
                            TmMetadata metadata = scaniaPositionRaw.getMetadata();

                            long created = dateParser.parse(scaniaPositionRaw.getCreatedDateTime()).getTime() / 1000;


                            // Create a new MixGeneralEvent from the raw event
                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created, dataSource,
                                    metadata, deviceId, deviceId);
                            generalEvent.setSpeed(speed);
                            generalEvent.setAltitude(altitude);
                            generalEvent.setDirection(direction);

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalEvent);

                            // Get the formatted event type (specific to the MIX category)
                            generalEvent.setType(formattedEvent);
                            generalEvent.setSourceType(eventType);

                            // Set control room IDs associated with the event
                            EventUtils.setControlRooms(generalEvent, metadata, dataSource);

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
