package za.co.trackmatic.flink.processors.scania;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.scania.GNSSPosition;
import za.co.trackmatic.flink.models.scania.ScaniaPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * Processor for Scania live position data.
 *
 * This class reads raw {@link ScaniaPositionRaw} events from Kafka, enriches them with geofence information,
 * status derived from speed, and additional metadata, then outputs enriched {@link GeneralLocation} events to Kafka.
 */
public class ScaniaLiveposProcessor implements Serializable {

    /** Default constructor for ScaniaLiveposProcessor. */
    public ScaniaLiveposProcessor() {
    }

    /**
     * Processes raw Scania position events, enriches them with geofence and status data,
     * and writes the enriched events to a Kafka sink.
     *
     * <p>The processing steps are:</p>
     * <ul>
     *   <li>Read raw {@link ScaniaPositionRaw} from the configured Kafka source.</li>
     *   <li>Parse timestamp strings to epoch seconds.</li>
     *   <li>Determine geofence membership for each event position.</li>
     *   <li>Calculate status based on the vehicle speed.</li>
     *   <li>Emit enriched {@link GeneralLocation} events to the configured Kafka sink.</li>
     * </ul>
     *
     * @param config The Flink job configuration containing Kafka source and sink details and other settings.
     * @throws Exception If the Flink job execution encounters an error.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Initialize a SimpleDateFormat to parse timestamp strings
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            // Configure Kafka source and deserializer for ScaniaPositionRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ScaniaPositionRaw> deserializer = new GenericDeserializer<>(ScaniaPositionRaw.class);
            KafkaSource<ScaniaPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for ScaniaPositionRaw data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw ScaniaPositionRaw data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "scania:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<ScaniaPositionRaw, GeneralLocation>() {
                        /**
                         * Enriches each raw Scania position event by:
                         * <ul>
                         *   <li>Parsing the timestamp string to epoch seconds.</li>
                         *   <li>Extracting position, speed, direction, and altitude.</li>
                         *   <li>Checking if the position lies inside any geofence and assigning geofence info.</li>
                         *   <li>Determining the vehicle status based on speed.</li>
                         *   <li>Emitting the enriched {@link GeneralLocation} event downstream.</li>
                         * </ul>
                         *
                         * @param scaniaPositionRaw The raw Scania position data event.
                         * @param collector The collector to emit the enriched events.
                         * @throws Exception If parsing or processing fails.
                         */
                        @Override
                        public void flatMap(ScaniaPositionRaw scaniaPositionRaw, Collector<GeneralLocation> collector) throws Exception {
                            GNSSPosition posData = scaniaPositionRaw.getGnssPosition();

                            double lat = posData.getLatitude();
                            double lon = posData.getLongitude();
                            double speed = scaniaPositionRaw.getWheelBasedSpeed();
                            long timestamp = dateParser.parse(scaniaPositionRaw.getCreatedDateTime()).getTime() / 1000;
                            TmMetadata metadata = scaniaPositionRaw.getMetadata();
                            String deviceId = String.valueOf(scaniaPositionRaw.getVin());
                            Double direction = posData.getHeading() != null ? Double.valueOf(posData.getHeading()) : null;
                            Double altitude = posData.getAltitude() != null ? posData.getAltitude().doubleValue() : null;

                            GeneralLocation generalLocation = new GeneralLocation(lat, lon, timestamp, SourceMapping.SCANIA, metadata, deviceId, deviceId);
                            generalLocation.setSpeed(speed);
                            generalLocation.setDirection(direction);
                            generalLocation.setAltitude(altitude);

                            // Check if the position is inside a geofence and set the geofence if found
                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalLocation);

                            // Set the status based on speed
                            String status = Utils.getStatus(speed);
                            generalLocation.setStatus(status);

                            // Emit the enriched event downstream
                            collector.collect(generalLocation);// Write the enriched event to Kafka sink
                        }
                    }).sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }

}
