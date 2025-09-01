package za.co.trackmatic.flink.processors.volvo;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.volvo.VolvoLiveposRaw;
import za.co.trackmatic.flink.serde.GenericListDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Processor class that enriches raw Volvo live position data by adding metadata,
 * then writes the enriched data back to a Kafka sink.
 */
public class VolvoLiveposEnricher {

    /**
     * Default constructor.
     */
    public VolvoLiveposEnricher() {
    }

    /**
     * Main processing method that reads raw Volvo live position data from Kafka,
     * enriches it with metadata fetched from the configuration,
     * and writes the enriched records back to Kafka.
     *
     * @param config The configuration containing Kafka source and sink details, and other settings.
     * @throws Exception If an error occurs during Flink job execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<VolvoLiveposRaw> deserializer = new GenericListDeserializer<>(VolvoLiveposRaw.class);
            KafkaSource<VolvoLiveposRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<VolvoLiveposRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<VolvoLiveposRaw> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "volvo:livepos-raw->enriched")
                    .flatMap(new FlatMapFunction<VolvoLiveposRaw, VolvoLiveposRaw>() {
                        /**
                         * Enriches each raw Volvo live position record with metadata.
                         *
                         * @param rawPositionData The raw Volvo live position data.
                         * @param collector The collector to emit enriched data.
                         * @throws Exception If an error occurs during enrichment.
                         */
                        @Override
                        public void flatMap(VolvoLiveposRaw rawPositionData, Collector<VolvoLiveposRaw> collector) throws Exception {
                            TmMetadata meta = Utils.getMetaData(rawPositionData.getVin(), config,"volvo-enricher");
                            if(meta == null){
                                return;
                            }
                            rawPositionData.setMetadata(meta);
                            collector.collect(rawPositionData);
                        }
                    })
                    .sinkTo(output);
            env.execute(config.getFlinkJobName());
        }
    }
}
