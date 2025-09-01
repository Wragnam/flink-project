package za.co.trackmatic.flink.processors.surfsight;

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
import za.co.trackmatic.flink.events.surfsight.SurfsightSpecificData;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.surfsight.SurfsightEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processes raw Surfsight event data by converting it into a general event format,
 * enriching it with geofence information and Surfsight-specific details,
 * and then sending the processed events to Kafka.
 */
public class SurfsightEventProcessor {

    /**
     * Default constructor for SurfsightEventProcessor.
     */
    public SurfsightEventProcessor() {
    }

    /**
     * Executes the Flink job which:
     * <ul>
     *   <li>Consumes raw Surfsight event data from a Kafka source.</li>
     *   <li>Converts raw Surfsight event data into {@link GeneralEvent} objects.</li>
     *   <li>Enriches events with geofence location information.</li>
     *   <li>Sets Surfsight-specific data (such as camera files) on the general event.</li>
     *   <li>Publishes the processed events to a Kafka sink.</li>
     * </ul>
     *
     * @param config The configuration object containing Kafka source and sink configurations
     *               and Flink job parameters.
     * @throws Exception If the Flink job execution or processing fails.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Configure Kafka source and deserializer for SurfsightEventRaw
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<SurfsightEventRaw> deserializer = new GenericDeserializer<>(SurfsightEventRaw.class);
            KafkaSource<SurfsightEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for SurfsightGeneralEvent
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Read data from Kafka source, apply transformations (flatMap), and write the result to Kafka sink
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ss:event-enriched->event")
                    .flatMap(new FlatMapFunction<SurfsightEventRaw, GeneralEvent>() {
                        /**
                         * Converts raw Surfsight event data to a {@link GeneralEvent},
                         * enriching it with geofence and Surfsight-specific information.
                         *
                         * @param rawSurfsightEventDataRaw The raw Surfsight event data.
                         * @param collector Collector to emit the processed GeneralEvent.
                         * @throws Exception If any error occurs during processing.
                         */
                        @Override
                        public void flatMap(SurfsightEventRaw rawSurfsightEventDataRaw, Collector<GeneralEvent> collector) throws Exception {
                            // Check if the raw event data is null, and return early if true
                            if (rawSurfsightEventDataRaw.getData() == null) {
                                // Skip processing if no event data is available
                                return;
                            }

                            double lat = rawSurfsightEventDataRaw.getData().getLat();
                            double lon = rawSurfsightEventDataRaw.getData().getLon();
                            TmMetadata metadata = rawSurfsightEventDataRaw.getMeta();
                            long created = rawSurfsightEventDataRaw.getData().getTime();
                            String deviceId = rawSurfsightEventDataRaw.getData().getSerialNumber();
                            double speed = rawSurfsightEventDataRaw.getData().getSpeed();
                            double altitude = rawSurfsightEventDataRaw.getData().getAlt();

                            // Create a new SurfsightGeneralEvent from the raw Surfsight event data
                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created,
                                    SourceMapping.SURFSIGHT, metadata, deviceId, deviceId);
                            generalEvent.setAltitude(altitude);
                            generalEvent.setSpeed(speed);

                            generalEvent.setSurfsight(new SurfsightSpecificData());
                            generalEvent.getSurfsight().setCameraFiles(rawSurfsightEventDataRaw.getData().getFiles());

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(), new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalEvent);

                            // Set the source type and the formatted event type for the general event
                            generalEvent.setSourceType(rawSurfsightEventDataRaw.getType());
                            generalEvent.setType(rawSurfsightEventDataRaw.getFormattedEvent());

                            // Emit the processed SurfsightGeneralEvent
                            collector.collect(generalEvent);
                        }
                    })
                    // Sink the converted SurfsightGeneralEvent data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }

    }
}
