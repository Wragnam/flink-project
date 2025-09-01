package za.co.trackmatic.flink.processors.volvo;

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
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.volvo.VolvoLiveposRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processor class for handling Volvo live position raw data,
 * enriching it with geofence information and status,
 * and forwarding the enriched events to Kafka.
 */
public class VolvoLiveposProcessor {

    /**
     * Processes raw Volvo live position data from Kafka source,
     * enriches it by setting metadata, geofence info, and status,
     * and writes enriched GeneralLocation events to Kafka sink.
     *
     * @param config The configuration object containing Kafka source, sink, and other parameters.
     * @throws Exception if any error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<VolvoLiveposRaw> deserializer = new GenericDeserializer<>(VolvoLiveposRaw.class);
            KafkaSource<VolvoLiveposRaw> input = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(input, WatermarkStrategy.noWatermarks(), "volvo:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<VolvoLiveposRaw, GeneralLocation>() {
                        /**
                         * Enriches each Volvo live position raw event by:
                         * - Parsing the timestamp,
                         * - Creating a GeneralLocation event with metadata,
                         * - Setting speed, direction, altitude,
                         * - Adding geofence info,
                         * - Determining status based on speed.
                         *
                         * @param rawPositionData The raw Volvo live position data.
                         * @param collector The collector to emit enriched GeneralLocation events.
                         * @throws Exception if any parsing or processing error occurs.
                         */
                        @Override
                        public void flatMap(VolvoLiveposRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {

                            double lat = rawPositionData.getGnssPosition().getLatitude();
                            double lon = rawPositionData.getGnssPosition().getLongitude();
                            TmMetadata metadata = rawPositionData.getMetadata();
                            Date date = dateParser.parse(rawPositionData.getCreatedDateTime());
                            String deviceId = rawPositionData.getVin();
                            double speed = rawPositionData.getGnssPosition().getSpeed();
                            Double direction = Double.valueOf(rawPositionData.getGnssPosition().getHeading());
                            Double altitude = Double.valueOf(rawPositionData.getGnssPosition().getAltitude());

                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, date.getTime() / 1000,
                                    SourceMapping.VOLVO, metadata, deviceId, deviceId);
                            generalPosition.setSpeed(speed);
                            generalPosition.setDirection(direction);
                            generalPosition.setAltitude(altitude);

                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, generalPosition.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalPosition);

                            String status = Utils.getStatus(rawPositionData.getGnssPosition().getSpeed());
                            generalPosition.setStatus(status);

                            collector.collect(generalPosition);
                        }
                    })
                    .sinkTo(output);
            env.execute(config.getFlinkJobName());
        }
    }
}
