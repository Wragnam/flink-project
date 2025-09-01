package za.co.trackmatic.flink.processors.udtrucks;

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
import za.co.trackmatic.flink.models.udtrucks.RawLivePos;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processes raw live position data from UD Trucks, enriches it with metadata,
 * geofence information, and status, then publishes enriched location events to Kafka.
 */
public class UdLivePosProcessor {

    /**
     * Processes the raw live position events from Kafka source, enriches with metadata and geofence data,
     * converts them into {@link GeneralLocation} events, and sends them to Kafka sink.
     *
     * @param config The configuration object containing Kafka source and sink details and other settings.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<RawLivePos> deserializer = new GenericDeserializer<>(RawLivePos.class);
            KafkaSource<RawLivePos> input = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(input, WatermarkStrategy.noWatermarks(), "ud:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<RawLivePos, GeneralLocation>() {

                        /**
                         * Transforms each raw live position event by enriching it with metadata,
                         * geofence location details, and status derived from speed.
                         *
                         * @param rawPositionData The raw live position data from UD Trucks.
                         * @param collector The collector used to emit enriched GeneralLocation events.
                         * @throws Exception If date parsing or enrichment fails.
                         */
                        @Override
                        public void flatMap(RawLivePos rawPositionData, Collector<GeneralLocation> collector) throws Exception {

                            double lat = rawPositionData.getPosition().getLatitude();
                            double lon = rawPositionData.getPosition().getLongitude();
                            TmMetadata metadata = rawPositionData.getTmMetadata();
                            Date date = dateParser.parse(rawPositionData.getTriggerTime());
                            String deviceId = rawPositionData.getVehicle().getVin();
                            String deviceSerial = rawPositionData.getVehicle().getChassisId();
                            double speed = rawPositionData.getPosition().getSpeed();
                            double altitude = rawPositionData.getPosition().getAltitude();
                            double direction = rawPositionData.getPosition().getHeading();

                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, date.getTime() / 1000,
                                    SourceMapping.UD_TRUCKS, metadata, deviceId, deviceSerial);
                            generalPosition.setSpeed(speed);
                            generalPosition.setDirection(direction);
                            generalPosition.setAltitude(altitude);

                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, generalPosition.getOrgId(), new LatLong(generalPosition.getLatitude(), generalPosition.getLongitude()), rawPositionData.getTmMetadata().getSiteIds());
                            Utils.setLocationNameAndId(fence, generalPosition);


                            String status = Utils.getStatus(rawPositionData.getPosition().getSpeed());
                            generalPosition.setStatus(status);

                            collector.collect(generalPosition);
                        }
                    })
                    .sinkTo(output);
            env.execute(config.getFlinkJobName());
        }
    }
}
