package za.co.trackmatic.flink.processors.lytx;

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
import za.co.trackmatic.flink.models.lytx.LytxPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Flink processor that converts enriched {@link LytxPositionRaw} live position data into {@link GeneralLocation} events.
 *
 * <p>The processor:
 * <ul>
 *   <li>Reads enriched Lytx live position events from Kafka.</li>
 *   <li>Parses timestamps and calculates the UNIX epoch.</li>
 *   <li>Evaluates current geofence inclusion for each point.</li>
 *   <li>Sets location metadata and vehicle status (e.g. moving/stopped).</li>
 *   <li>Emits transformed {@link GeneralLocation} events to a Kafka sink.</li>
 * </ul>
 */
public class LytxLiveposProcessor {

    /**
     * Default constructor.
     */
    public LytxLiveposProcessor() {

    }

    /**
     * Processes enriched Lytx live position data, transforms it to {@link GeneralLocation} format,
     * and publishes it to the output Kafka topic.
     *
     * @param config The {@link Config} object that includes Kafka and Flink settings.
     * @throws Exception if any error occurs during stream processing.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SX");
            SimpleDateFormat dateParser2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LytxPositionRaw> deserializer = new GenericDeserializer<>(LytxPositionRaw.class);
            KafkaSource<LytxPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralLocation> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralLocation> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lytx:livepos-enriched->livepos")
                    .flatMap(new FlatMapFunction<LytxPositionRaw, GeneralLocation>() {

                        /**
                         * Transforms a {@link LytxPositionRaw} into a {@link GeneralLocation} by enriching
                         * it with timestamp, geofence, status, and metadata.
                         *
                         * @param rawPositionData The raw enriched live position data from Lytx.
                         * @param collector The Flink collector used to emit the processed {@link GeneralLocation}.
                         * @throws Exception if an error occurs while parsing timestamp or enriching data.
                         */
                        @Override
                        public void flatMap(LytxPositionRaw rawPositionData, Collector<GeneralLocation> collector) throws Exception {

                            double lat = rawPositionData.getLatitude();
                            double lon = rawPositionData.getLongitude();
                            TmMetadata metadata = rawPositionData.getMetadata();
                            String deviceId = rawPositionData.getDcVehicleId();
                            Double speed = rawPositionData.getSpeed();

                            GeneralLocation generalPosition = new GeneralLocation(lat, lon, 0, SourceMapping.LYTX, metadata,
                                    deviceId, deviceId);
                            generalPosition.setSpeed(speed);

                            try {
                                Date date = dateParser.parse(rawPositionData.getTimestamp());
                                generalPosition.setTimestamp(date.getTime() / 1000);

                            } catch (Exception e) {
                                Date date = dateParser2.parse(rawPositionData.getTimestamp());
                                generalPosition.setTimestamp(date.getTime() / 1000);
                            }

                            String server = config.getDatacacheServer();
                            GeofenceItem fence = CacheUtils.isPointInGeofence(server, rawPositionData.getMetadata().getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalPosition);

                            String status = Utils.getStatus(speed);
                            generalPosition.setStatus(status);

                            collector.collect(generalPosition);
                        }
                    })
                    .sinkTo(output);

            env.execute(config.getFlinkJobName());
        }
    }
}
