package za.co.trackmatic.flink.processors.fleetboard;

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
import za.co.trackmatic.flink.models.fleetboard.FleetboardPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class processes raw Fleetboard position data, enriches it with additional metadata,
 * and outputs enriched live position data to Kafka.
 */
public class FleetboardLiveposProcessor {

    /**
     * Default constructor for the FleetboardLiveposProcessor class.
     */
    public FleetboardLiveposProcessor() {}

    /**
     * Processes the raw Fleetboard position data and enriches it with metadata before outputting
     * the enriched data to a Kafka sink.
     *
     * @param config the configuration containing source and sink Kafka information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Date format to parse the timestamp from raw position data
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<FleetboardPositionRaw> deserializer = new GenericDeserializer<>(FleetboardPositionRaw.class);
            KafkaSource<FleetboardPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for enriched live position data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw position data
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "fb:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<FleetboardPositionRaw, GeneralLocation>() {
                        /**
                         * Processes raw position data and enriches it with metadata before emitting it as a
                         * FleetboardLiveposEvent.
                         *
                         * @param rawPositionData the raw position data to be enriched
                         * @param collector the collector to emit the enriched position data
                         * @throws Exception if an error occurs during the enrichment process
                         */
                        @Override
                        public void flatMap(FleetboardPositionRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {

                            double lat = rawPositionData.getLat();
                            double lon = rawPositionData.getLon();
                            TmMetadata metadata = rawPositionData.getMetaData();
                            String deviceId = rawPositionData.getVehicleId();
                            String[] positionData = rawPositionData.getPosText().split(",", 4);


                            // Parse the timestamp and set it as a Unix timestamp (seconds)
                            Date date = dateParser.parse(rawPositionData.getTimestamp());
                            long timestamp = date.getTime() / 1000;

                            // Create a new live position event based on the raw position data
                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, timestamp, SourceMapping.FLEETBOARD, metadata, deviceId, deviceId);

                            String orientation = null;
                            // Split position text (orientation, speed, position)
                            if (positionData.length >= 1) {
                                orientation = positionData[0];
                            }
                            if (positionData.length >= 2) {
                                generalPosition.setSpeed(Double.valueOf(positionData[1]));
                            }

                            // Check if the position is within any geofence
                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalPosition);

                            // Determine the status based on speed (e.g., "Moving", "Idle")
                            String status = Utils.getStatus(rawPositionData.getSpeed().doubleValue());
                            generalPosition.setStatus(status);

                            // If orientation is available, calculate the direction of the vehicle
                            if (orientation != null && !orientation.isEmpty()) {
                                double direction = Utils.calculateDirection(orientation);
                                generalPosition.setDirection(direction);
                            }

                            // Emit the enriched position data
                            collector.collect(generalPosition);
                        }
                    })
                    // Output the enriched position data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
