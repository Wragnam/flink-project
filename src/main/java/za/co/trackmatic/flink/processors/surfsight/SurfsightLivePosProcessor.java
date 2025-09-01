package za.co.trackmatic.flink.processors.surfsight;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.*;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralLocation;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.surfsight.SurfsightPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.util.List;

/**
 * Processor for Surfsight live position data.
 * <p>
 * This class reads raw Surfsight live position data from a Kafka source,
 * enriches the data with geofence information and status based on speed,
 * and writes the enriched live position events back to a Kafka sink.
 * </p>
 */
public class SurfsightLivePosProcessor {

    /**
     * Processes raw Surfsight live position data from Kafka, enriches each event
     * with geofence and status information, and publishes the enriched data back to Kafka.
     *
     * @param config Configuration object containing Kafka source and sink details,
     *               as well as other necessary configuration parameters.
     * @throws Exception If the Flink job encounters any execution errors.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for SurfsightPositionRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<SurfsightPositionRaw> deserializer = new GenericDeserializer<>(SurfsightPositionRaw.class);
            KafkaSource<SurfsightPositionRaw> input = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for SurfsightLiveposEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            // Read data from Kafka source, apply transformations (flatMap), and write the result to Kafka sink
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "ss:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<SurfsightPositionRaw, GeneralLocation>() {

                        /**
                         * Enriches raw Surfsight live position data by:
                         * <ul>
                         *   <li>Extracting location and speed information</li>
                         *   <li>Setting geofence info based on the position</li>
                         *   <li>Determining status based on speed</li>
                         *   <li>Populating the enriched GeneralLocation event</li>
                         * </ul>
                         *
                         * @param rawPositionData The raw Surfsight position data.
                         * @param collector Collector to emit the enriched GeneralLocation event.
                         * @throws Exception If an error occurs during enrichment.
                         */
                        @Override
                        public void flatMap(SurfsightPositionRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {
                            double lat = rawPositionData.getLat();
                            double lon = rawPositionData.getLon();
                            double speed = rawPositionData.getSpeed();
                            double altitude = rawPositionData.getAlt();
                            double accuracy = rawPositionData.getAccuracy();
                            TmMetadata metadata = rawPositionData.getMeta();
                            String deviceId = rawPositionData.getSerialNumber();


                            // Create a new SurfsightLiveposEvent from the raw position data
                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, rawPositionData.getTime(),
                                    SourceMapping.SURFSIGHT, metadata, deviceId, deviceId);

                            generalPosition.setSpeed(speed);
                            generalPosition.setAccuracy(accuracy);
                            generalPosition.setAltitude(altitude);

                            // Get the server URL and fetch geofence data for the position
                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, generalPosition.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalPosition);

                            // Determine the status based on the speed of the position
                            String status = Utils.getStatus(rawPositionData.getSpeed());
                            generalPosition.setStatus(status);

                            // Emit the enriched live position event
                            collector.collect(generalPosition);
                        }
                    })
                    // Sink the enriched SurfsightLiveposEvent data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
