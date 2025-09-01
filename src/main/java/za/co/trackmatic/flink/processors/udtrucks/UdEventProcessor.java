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
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.udtrucks.UdDrivingBehaviour;
import za.co.trackmatic.flink.models.udtrucks.UdEventRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;

/**
 * Processes raw UD Trucks events by extracting driving behaviour events,
 * converting them into general events enriched with metadata and geofence data,
 * and publishing the processed events to a Kafka sink.
 */
public class UdEventProcessor {

    /**
     * Default constructor for UdEventProcessor.
     */
    public UdEventProcessor() {

    }

    /**
     * Processes raw UD Trucks events read from Kafka, extracts individual driving behaviour events,
     * enriches them with metadata and geofence information, and writes the enriched events back to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details as well as other settings.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<UdEventRaw> deserializer = new GenericDeserializer<>(UdEventRaw.class);
            KafkaSource<UdEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ud:event-enriched->event").flatMap(new FlatMapFunction<UdEventRaw, GeneralEvent>() {
                @Override
                public void flatMap(UdEventRaw udRawEventData, Collector<GeneralEvent> collector) throws Exception {
                    String serial = Utils.cleanSerial(udRawEventData.getVehicle().getVin());
                    if (Objects.equals(serial, Utils.UNKNOWN)) {
                        return;
                    }

                    String deviceSerial = udRawEventData.getVehicle().getChassisId();
                    TmMetadata metadata = udRawEventData.getMetadata();

                    for (UdDrivingBehaviour dataItem : udRawEventData.getDrivingBehaviourEvents()) {
                        String formattedEvent = Utils.getFormattedEvent(dataItem.getTriggerType(), "UDTRUCKS");
                        if (formattedEvent == null) {
                            continue;
                        }

                        double lat = dataItem.getPosition().getLatitude();
                        double lon = dataItem.getPosition().getLongitude();
                        double direction = dataItem.getPosition().getHeading();
                        double speed = dataItem.getPosition().getSpeed();
                        double altitude = dataItem.getPosition().getAltitude();
                        Date date = dateParser.parse(dataItem.getTriggerTime());


                        GeneralEvent generalEvent = new GeneralEvent(lat, lon, date.getTime() / 1000,
                                SourceMapping.UD_TRUCKS, metadata, serial, deviceSerial);
                        generalEvent.setSpeed(speed);
                        generalEvent.setDirection(direction);
                        generalEvent.setAltitude(altitude);

                        generalEvent.setType(formattedEvent);

                        GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                        Utils.setLocationNameAndId(fence, generalEvent);


                        collector.collect(generalEvent);
                    }
                }
            }).sinkTo(output);

            env.execute(config.getFlinkJobName());
        }
    }
}
