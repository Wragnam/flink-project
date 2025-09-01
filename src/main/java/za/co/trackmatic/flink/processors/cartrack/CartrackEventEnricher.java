package za.co.trackmatic.flink.processors.cartrack;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.SourceMapping;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.cartrack.CartrackEventRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericListDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * A processor class that enriches raw Cartrack events by adding metadata,
 * filtering based on event behavior, and then outputs the enriched events to Kafka.
 */
public class CartrackEventEnricher {

    /**
     * Default constructor for `CartrackEventEnricher`.
     */
    public CartrackEventEnricher() {
    }

    /**
     * Processes the raw Cartrack event data by enriching it with metadata and filtering based on behaviors.
     * The enriched data is then sent to Kafka.
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
            GenericListDeserializer<CartrackEventRaw> deserializer = new GenericListDeserializer<>(CartrackEventRaw.class);
            KafkaSource<CartrackEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Kafka sink configuration and serialization setup for enriched Cartrack event data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<CartrackEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<CartrackEventRaw> output = Utils.createKafkaSink(sink, serializer);

            // Stream processing: enrich raw Cartrack event data by adding metadata and filtering based on behaviors

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ct:event-raw->event-enriched").flatMap(new FlatMapFunction<CartrackEventRaw, CartrackEventRaw>() {
                        /**
                         * Transforms and enriches raw Cartrack event data by adding metadata and filtering based on event behaviors.
                         *
                         * @param cartrackEventRaw the raw Cartrack event data to be processed
                         * @param collector collects the enriched CartrackEventRaw for further processing or output
                         * @throws Exception if an error occurs during the transformation
                         */
                        @Override
                        public void flatMap(CartrackEventRaw cartrackEventRaw, Collector<CartrackEventRaw> collector) throws Exception {
                            String formattedEvent = Utils.getFormattedEvent(cartrackEventRaw.getEventDescription(), SourceMapping.CARTRACK);
                            if (formattedEvent == null) {
                                return;
                            }

                            // Retrieve metadata for the vehicle associated with the event
                            TmMetadata meta = Utils.getMetaData(String.valueOf(cartrackEventRaw.getVehicleId()), config, "ct-ev-enricher");
                            if (meta == null) {
                                return;
                            }

                            // Set the enriched metadata to the Cartrack event
                            cartrackEventRaw.setMetadata(meta);
                            cartrackEventRaw.setEventName(formattedEvent);

                            // Collect the enriched event for further processing
                            collector.collect(cartrackEventRaw);
                        }
                    })
                    // Sink the enriched Cartrack event data to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the specified job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}
