package za.co.trackmatic.flink.processors.icam;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.icam.IcamPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;

/**
 * This class processes raw ICAM position data, enriches it with geofence information and status,
 * and outputs the enriched data to Kafka as live position events.
 */
public class IcamLiveposProcessor {

    /**
     * Processes the raw ICAM position data, enriches it with geofence and status information,
     * and outputs the enriched data to Kafka as live position events.
     *
     * @param config the configuration containing source and sink Kafka information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Define date format to parse timestamps in raw position data
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

            // Create Kafka source for raw position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<IcamPositionRaw> deserializer = new GenericDeserializer<>(IcamPositionRaw.class);
            KafkaSource<IcamPositionRaw> input = Utils.createKafkaSource(source, deserializer);

            // Create Kafka sink for live position events
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw position data
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "icam:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<IcamPositionRaw, GeneralLocation>() {
                        /**
                         * Enriches the raw position data by adding geofence and status information,
                         * and emits the enriched data as a live position event.
                         *
                         * @param rawPositionData the raw position data to be enriched
                         * @param collector the collector to emit the enriched live position event
                         * @throws Exception if an error occurs during the enrichment process
                         */
                        @Override
                        public void flatMap(IcamPositionRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {

                            double lat = rawPositionData.getLatitude();
                            double lon = rawPositionData.getLongitude();
                            double direction = rawPositionData.getCourse();
                            double altitude = rawPositionData.getAltitude();
                            double speed = rawPositionData.getSpeed();
                            TmMetadata metadata = rawPositionData.getMetadata();
                            long time = Utils.getUTCTimeFromZone(dateParser.parse(rawPositionData.getTimestamp()), "+2");
                            String deviceId = String.valueOf(rawPositionData.getId());

                            // Create a general position event from the raw data
                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, time, "ICAM", metadata, deviceId, deviceId);
                            generalPosition.setSpeed(speed);
                            generalPosition.setAltitude(altitude);
                            generalPosition.setDirection(direction);

                            // Retrieve the datacache server from the configuration
                            String server = config.getDatacacheServer();

                            // Check if the raw position is inside any geofences
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, metadata.getOrgId(), new LatLong(rawPositionData.getLatitude(), rawPositionData.getLongitude()), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalPosition);

                            // Set the status of the general position based on speed
                            String status = Utils.getStatus(rawPositionData.getSpeed());
                            generalPosition.setStatus(status);

                            // Emit the enriched live position event
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
