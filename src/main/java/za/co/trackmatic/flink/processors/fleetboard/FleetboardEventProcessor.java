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
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.fleetboard.FleetboardEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processes raw Fleetboard event data, enriches it, and writes the enriched events to a Kafka sink.
 * <p>
 * This class reads raw Fleetboard event data from a Kafka source, formats and enriches it,
 * then sends the enriched Fleetboard events to a Kafka sink.
 */
public class FleetboardEventProcessor {

    private static final String dataSource = SourceMapping.FLEETBOARD;

    /**
     * Default constructor for the FleetboardEventProcessor class.
     */
    public FleetboardEventProcessor() {

    }

    /**
     * Processes the raw Fleetboard event data, enriches it, and sends the enriched data to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // SimpleDateFormat to parse timestamp strings from raw events
            SimpleDateFormat dateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<FleetboardEventRaw> deserializer = new GenericDeserializer<>(FleetboardEventRaw.class);
            KafkaSource<FleetboardEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich the event data, and write back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "fb:event-enriched->event")
                    .flatMap(new FlatMapFunction<FleetboardEventRaw, GeneralEvent>() {
                        /**
                         * Enriches the raw Fleetboard event data by formatting the event and parsing the timestamp.
                         * Sets source type, event type, and timestamp in the enriched event.
                         * If valid, the enriched event is forwarded to the output stream.
                         *
                         * @param rawEventData the raw Fleetboard event data to process
                         * @param collector the collector to output the enriched FleetboardGeneralEvent
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(FleetboardEventRaw rawEventData, Collector<GeneralEvent> collector) throws Exception {

                            TmMetadata metadata = rawEventData.getMetaData();
                            double lat = rawEventData.getLat();
                            double lon = rawEventData.getLon();
                            String deviceId = rawEventData.getVehicleId();
                            Double direction = rawEventData.getCourse() != null ? rawEventData.getCourse().doubleValue() : null;

                            // Parse the timestamp from the raw event and set it in the general event
                            Date date = dateParser.parse(rawEventData.getTimestamp());
                            long created = date.getTime() / 1000;

                            String[] position = rawEventData.getPositionText().split(",", 4);
                            Double speed = Double.valueOf(position[1]);

                            // Create a new FleetboardGeneralEvent based on the raw event data
                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created, dataSource, metadata, deviceId, deviceId);
                            generalEvent.setDirection(direction);
                            generalEvent.setSpeed(speed);

                            String type = rawEventData.getEventType().toString();

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());

                            Utils.setLocationNameAndId(fence, generalEvent);

                            generalEvent.setSourceType(type);
                            generalEvent.setType(rawEventData.getFormattedEvent());

                            // Collect the enriched event data for further processing
                            collector.collect(generalEvent);
                        }
                    })
                    // Sink the enriched event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }

    }
}
