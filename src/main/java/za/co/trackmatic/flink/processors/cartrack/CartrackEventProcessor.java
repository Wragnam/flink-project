package za.co.trackmatic.flink.processors.cartrack;

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
import za.co.trackmatic.flink.models.cartrack.CartrackEventRaw;
import za.co.trackmatic.flink.models.trackmatic.GeofenceItem;
import za.co.trackmatic.flink.models.trackmatic.LatLong;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Processor that consumes raw Cartrack events, transforms and enriches them with metadata,
 * location info, and other details, then outputs enriched GeneralEvent objects to Kafka.
 */
public class CartrackEventProcessor {

    /**
     * Default constructor for `CartrackEventProcessor`.
     */
    public CartrackEventProcessor() {
    }

    private static final String originalDateFormat = "yyyy-MM-dd HH:mm:ssXXX";

    private static final String dataSource = SourceMapping.CARTRACK;

    private static final SimpleDateFormat dateParser = new SimpleDateFormat(originalDateFormat);

    /**
     * Processes the raw Cartrack event data by performing necessary transformations,
     * and sending it to Kafka.
     *
     * @param config the configuration containing Kafka source and sink details, as well as other settings
     * @throws Exception if any error occurs during processing or job execution
     */
    public void process(Config config) throws Exception {
        // Set up the Flink stream execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set the generic data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            // Kafka source configuration and deserialization setup for raw Cartrack event data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<CartrackEventRaw> deserializer = new GenericDeserializer<>(CartrackEventRaw.class);
            KafkaSource<CartrackEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Kafka sink configuration and serialization setup for enriched Cartrack event data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Stream processing: enrich raw Cartrack event data by adding metadata and filtering based on behaviors

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ct:event-raw->event-enriched").flatMap(new FlatMapFunction<CartrackEventRaw, GeneralEvent>() {
                        /**
                         * Transforms and enriches raw Cartrack event data by adding metadata and filtering based on event behaviors.
                         *
                         * @param cartrackEventRaw the raw Cartrack event data to be processed
                         * @param collector collects the enriched CartrackEventRaw for further processing or output
                         * @throws Exception if an error occurs during the transformation
                         */
                        @Override
                        public void flatMap(CartrackEventRaw cartrackEventRaw, Collector<GeneralEvent> collector) throws Exception {
                            Double lat = cartrackEventRaw.getLatitude();
                            Double lon = cartrackEventRaw.getLongitude();
                            String serial = String.valueOf(cartrackEventRaw.getVehicleId());
                            TmMetadata metadata = cartrackEventRaw.getMetadata();
                            Double altitude = cartrackEventRaw.getAltitude() != null ? cartrackEventRaw.getAltitude().doubleValue() : null;
                            Double speed = cartrackEventRaw.getSpeed().doubleValue();
                            Double direction = cartrackEventRaw.getBearing() != null ? cartrackEventRaw.getBearing().doubleValue() : null;

                            String normalizedDate = Utils.normalizeDateOffset(cartrackEventRaw.getEventTs());

                            Date date = dateParser.parse(normalizedDate);

                            long created = date.getTime() / 1000;

                            GeneralEvent generalEvent = new GeneralEvent(lat, lon, created, dataSource, metadata, serial, serial);
                            generalEvent.setType(cartrackEventRaw.getEventName());
                            generalEvent.setSourceType(cartrackEventRaw.getEventDescription());
                            generalEvent.setAltitude(altitude);
                            generalEvent.setSpeed(speed);
                            generalEvent.setDirection(direction);
                            generalEvent.setDate(Utils.convertDate(normalizedDate, originalDateFormat, Utils.dateFormat));
                            generalEvent.setIgnition(cartrackEventRaw.getIgnition());

                            GeofenceItem fence = CacheUtils.isPointInGeofence(config.getDatacacheServer(), metadata.getOrgId(),
                                    new LatLong(lat, lon), metadata.getSiteIds());
                            Utils.setLocationNameAndId(fence, generalEvent);

                            EventUtils.setControlRooms(generalEvent, metadata, dataSource);
                            // Collect the processed event
                            collector.collect(generalEvent);
                        }
                    })
                    // Sink the enriched Cartrack event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the specified job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
