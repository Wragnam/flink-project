package za.co.trackmatic.flink.processors.blackberry;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.CacheUtils;
import za.co.trackmatic.flink.Utils.EventUtils;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.blackberry.BlackberryEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processes Blackberry event data, enriches it with additional information, and sends the enriched events to a Kafka sink.
 * <p>
 * This class reads raw Blackberry events from a Kafka source, processes them by extracting and enriching relevant details,
 * and writes the resulting enriched events to a Kafka sink.
 */
public class BlackberryEventProcessor {

    private static final String dataSource = SourceMapping.BLACKBERRYRADAR;

    /**
     * Constructs a new BlackberryEventProcessor instance.
     */
    public BlackberryEventProcessor() {

    }

    /**
     * Processes raw Blackberry event data by enriching it with additional metadata and then sends it to a Kafka sink.
     *
     * @param config the configuration containing Kafka source and sink information
     * @throws Exception if any error occurs during the Flink job execution
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set stream options, such as checkpointing and parallelism
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure the Kafka source using the provided configuration
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<BlackberryEventRaw> deserializer = new GenericDeserializer<>(BlackberryEventRaw.class);
            KafkaSource<BlackberryEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure the Kafka sink using the provided configuration
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Processing pipeline: Read from Kafka, enrich events, and write back to Kafka
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "bb:event-enriched->event")
                    .flatMap(new FlatMapFunction<BlackberryEventRaw, GeneralEvent>() {
                        /**
                         * Enriches the raw Blackberry event and forwards it to the output stream.
                         *
                         * @param rawEventData the raw Blackberry event to process
                         * @param collector the collector to output the enriched BlackberryGeneralEvent
                         * @throws Exception if any error occurs during event processing
                         */
                        @Override
                        public void flatMap(BlackberryEventRaw rawEventData, Collector<GeneralEvent> collector) throws Exception {
                            // Clean the serial number from the event data
                            String serial = Utils.cleanSerial(rawEventData.getAssetid());
                            if (serial.equals(Utils.UNKNOWN)) {
                                return;
                            }

                            String type = rawEventData.getType();

                            double lat = rawEventData.getGeo_location().getLat();
                            double lon = rawEventData.getGeo_location().getLon();
                            long created = rawEventData.getRef_recorded_on() / 1000;
                            String deviceId = rawEventData.getDeviceId();
                            String deviceSerial = rawEventData.getIdentifier();
                            TmMetadata metadata = rawEventData.getTmMetadata();

                            // Create a new BlackberryGeneralEvent instance from the raw event data
                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created, dataSource, metadata, deviceId, deviceSerial);

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalEvent);

                            // Set the source type and control room IDs for the enriched event
                            generalEvent.setSourceType(type);
                            EventUtils.setControlRooms(generalEvent, metadata, dataSource);

                            // Set the formatted event type
                            generalEvent.setType(rawEventData.getFormattedEvent());

                            // Collect the enriched event for further processing
                            collector.collect(generalEvent);
                        }
                    })
                    // Sink the enriched event to Kafka
                    .sinkTo(output);

            // Execute the Flink job
            env.execute(config.getFlinkJobName());
        }
    }
}