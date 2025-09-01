package za.co.trackmatic.flink.processors.blackberry;

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
import za.co.trackmatic.flink.models.blackberry.BlackberryPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processes raw Blackberry position data, enriches it with geofence information,
 * and writes the enriched data to a Kafka sink for further processing.
 * <p>
 * This class reads raw Blackberry position data from a Kafka source, enriches the data with geofence information based on the
 * position's latitude and longitude, and writes the enriched data to a Kafka sink.
 */
public class BlackberryLiveposProcessor {
    /**
     * Processes the raw Blackberry position data, enriches it with geofence information, and sends it to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<BlackberryPositionRaw> deserializer = new GenericDeserializer<>(BlackberryPositionRaw.class);
            KafkaSource<BlackberryPositionRaw> input = Utils.createKafkaSource(source, deserializer);


            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich the position data, and write back to Kafka
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "bb:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<BlackberryPositionRaw, GeneralLocation>() {
                        /**
                         * Enriches the raw Blackberry position data with geofence information and forwards it to the output stream.
                         *
                         * @param rawPositionData the raw Blackberry position data to process
                         * @param collector the collector to output the enriched BlackberryLiveposEvent
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(BlackberryPositionRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {
                            Double lat = rawPositionData.getGeo_location().getLat();
                            Double lon = rawPositionData.getGeo_location().getLon();
                            long timeStamp = rawPositionData.getCreated() / 1000;
                            String deviceId = rawPositionData.getDeviceId();
                            String deviceSerial = rawPositionData.getAsset().getParam().getIdentifier();
                            TmMetadata metadata = rawPositionData.getMetadata();

                            // Create a new BlackberryLiveposEvent from the raw position data
                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, timeStamp, "BLACKBERRYRADAR",
                                    metadata, deviceId, deviceSerial
                            );
                            generalPosition.setAltitude(rawPositionData.getAltitude());

                            // Retrieve the geofence information based on the position's latitude and longitude
                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, generalPosition.getOrgId(), new LatLong(generalPosition.getLatitude(),
                                    generalPosition.getLongitude()), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalPosition);

                            // Collect the enriched position data for further processing
                            collector.collect(generalPosition);
                        }
                    })
                    // Sink the enriched position data to Kafka
                    .sinkTo(output);
            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}
