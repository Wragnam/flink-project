package za.co.trackmatic.flink.processors.lytx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.lytx.LytxPositionRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor that enriches incoming raw {@link LytxPositionRaw} live position events
 * with {@link TmMetadata} and publishes them to a Kafka topic.
 *
 * <p>This class reads Lytx live position data from a configured Kafka source topic,
 * enriches each record with additional metadata retrieved from cache (or DB fallback),
 * and writes the enriched data to a Kafka sink topic.</p>
 */
public class LytxLiveposEnricher {

    /**
     * Default constructor for {@code LytxLiveposEnricher}.
     */
    public LytxLiveposEnricher(){

    }

    /**
     * Starts the Flink streaming job for enriching Lytx live position data.
     *
     * <p>This sets up the Flink environment, consumes data from a Kafka topic,
     * enriches each {@link LytxPositionRaw} message with metadata, and sinks the
     * enriched data to another Kafka topic.</p>
     *
     * @param config the {@link Config} object containing Flink job name, Kafka source/sink settings,
     *               and cache service configuration.
     * @throws Exception if there is an error during Flink job setup or execution.
     */
    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LytxPositionRaw> deserializer = new GenericDeserializer<>(LytxPositionRaw.class);
            KafkaSource<LytxPositionRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<LytxPositionRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<LytxPositionRaw> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "lytx:lp-raw->lp-enriched").flatMap(new FlatMapFunction<LytxPositionRaw, LytxPositionRaw>() {
                /**
                 * Enriches a raw {@link LytxPositionRaw} event with metadata and emits the result.
                 *
                 * @param rawPositionData the incoming raw position event
                 * @param collector the collector used to emit the enriched result downstream
                 * @throws Exception if enrichment fails
                 */
                @Override
                public void flatMap(LytxPositionRaw rawPositionData, Collector<LytxPositionRaw> collector) throws Exception {
                    TmMetadata meta = Utils.getMetaData(rawPositionData.getDcVehicleId(),config,"lytx-livepos-enricher");
                    if(meta==null){
                        return;
                    }
                    rawPositionData.setMetadata(meta);

                    collector.collect(rawPositionData);
                }
            }).sinkTo(output);

            env.execute(config.getFlinkJobName());
        }
    }
}
