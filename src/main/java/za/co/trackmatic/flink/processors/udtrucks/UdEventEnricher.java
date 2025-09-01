package za.co.trackmatic.flink.processors.udtrucks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Logger;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.udtrucks.UdEventRaw;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Enriches raw UD Trucks event data by attaching metadata and filtering events.
 * Reads raw events from Kafka, enriches them with metadata based on VIN,
 * and outputs the enriched events back to Kafka.
 */
public class UdEventEnricher {

    /**
     * Default constructor.
     */
    public UdEventEnricher() {

    }

    /**
     * Processes raw UD Trucks events from Kafka by enriching each event with metadata,
     * filtering out events with empty driving behaviour, and writing the enriched events
     * back to Kafka.
     *
     * @param config Configuration containing Kafka source and sink settings.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<UdEventRaw> deserializer = new GenericDeserializer<>(UdEventRaw.class);
            KafkaSource<UdEventRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<UdEventRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<UdEventRaw> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ud:event-raw->event-enriched").flatMap(new FlatMapFunction<UdEventRaw, UdEventRaw>() {
                @Override
                public void flatMap(UdEventRaw udEventRaw, Collector<UdEventRaw> collector) throws Exception {
                    Logger.log("IN UD EVENT ENRICHER", 5557);
                    if(udEventRaw.getDrivingBehaviourEvents().isEmpty()){
                        return;
                    }

                    TmMetadata meta = Utils.getMetaData(udEventRaw.getVehicle().getVin(),config,"ud-ev-enricher");
                    if(meta==null){
                        Logger.log("METADATA FOR " + udEventRaw.getVehicle().getVin() + " is empty", 5557);
                        return;
                    }

                    udEventRaw.setMetadata(meta);

                    Logger.log("Collecting enriched UD data", 5557);

                    collector.collect(udEventRaw);
                }
            }).sinkTo(output);

            env.execute(config.getFlinkJobName());
        }
    }
}
