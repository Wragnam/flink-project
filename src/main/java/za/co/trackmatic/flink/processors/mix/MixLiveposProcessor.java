package za.co.trackmatic.flink.processors.mix;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.mix.MixPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.io.Serializable;
import java.text.SimpleDateFormat;

/**
 * Flink processor to enrich raw Mix position events with geofence and status data.
 *
 * <p>This processor consumes {@link MixPositionRaw} events from Kafka, enriches them by parsing timestamps,
 * calculating accuracy based on satellite count, determining geofence membership, and setting status
 * based on speed. The enriched {@link GeneralLocation} events are then emitted to a Kafka sink.</p>
 */
public class MixLiveposProcessor implements Serializable {

    /**
     * Default constructor for {@code MixLiveposProcessor}.
     */
    public MixLiveposProcessor() {
    }

    /**
     * Main method to process raw position events, enrich them with geofence and status data,
     * and write them to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Initialize a SimpleDateFormat to parse timestamp strings
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            // Configure Kafka source and deserializer for MixPositionRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<MixPositionRaw> deserializer = new GenericDeserializer<>(MixPositionRaw.class);
            KafkaSource<MixPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for MixLiveposEvent data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw MixPositionRaw data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "mix:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<MixPositionRaw, GeneralLocation>() {
                        /**
                         * Enriches each `MixPositionRaw` event by:
                         * - Parsing the timestamp.
                         * - Checking if the event is inside a geofence.
                         * - Setting the accuracy based on the number of satellites.
                         * - Determining the status based on speed.
                         *
                         * @param mixPositionRaw The raw position event to be enriched.
                         * @param collector The collector used to emit the enriched event downstream.
                         * @throws Exception If any error occurs during the enrichment process.
                         */
                        @Override
                        public void flatMap(MixPositionRaw mixPositionRaw, Collector<GeneralLocation> collector) throws Exception {
                            double lat = mixPositionRaw.getLatitude();
                            double lon = mixPositionRaw.getLongitude();
                            double speed = mixPositionRaw.getSpeedKilometresPerHour();
                            long timestamp = dateParser.parse(mixPositionRaw.getTimestamp()).getTime() / 1000;
                            TmMetadata metadata = mixPositionRaw.getMetadata();
                            String deviceId = String.valueOf(mixPositionRaw.getAssetId());
                            double accuracy = ((double) mixPositionRaw.getNumberOfSatellites() / 12) * 100;
                            double direction = mixPositionRaw.getHeading();
                            double altitude = mixPositionRaw.getAltitudeMetres();


                            // Create a new MixLiveposEvent from the raw position event
                            GeneralLocation generalLocation = new GeneralLocation(lat, lon, timestamp, SourceMapping.MIX, metadata, deviceId, deviceId);
                            generalLocation.setSpeed(speed);
                            generalLocation.setAccuracy(accuracy);
                            generalLocation.setDirection(direction);
                            generalLocation.setAltitude(altitude);

                            // Check if the position is inside a geofence and set the geofence if found
                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, mixPositionRaw.getMetadata().getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalLocation);

                            // Set the status based on speed
                            String status = Utils.getStatus(mixPositionRaw.getSpeedKilometresPerHour());
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
