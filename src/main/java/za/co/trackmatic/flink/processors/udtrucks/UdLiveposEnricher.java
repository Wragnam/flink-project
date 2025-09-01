package za.co.trackmatic.flink.processors.udtrucks;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.models.udtrucks.RawLivePos;
import za.co.trackmatic.flink.serde.GenericListDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Takes raw udtrucks livepos data and adds trackmatic fields like orgId, assetId, etc.
 * This is typically the starting point i.e. all other operations depend on the enriched
 * data
 */
public class UdLiveposEnricher {

    public UdLiveposEnricher() {
    }

    public void process(Config config) throws Exception {
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            Utils.setGenericDataStreamOptions(env, 60000, 1000);

            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<RawLivePos> deserializer = new GenericListDeserializer<>(RawLivePos.class);
            KafkaSource<RawLivePos> rawInput = Utils.createKafkaSource(source, deserializer);

            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<RawLivePos> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<RawLivePos> output = Utils.createKafkaSink(sink, serializer);

            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "ud:livepos-raw->enriched")
                    .flatMap(new FlatMapFunction<RawLivePos, RawLivePos>() {
                        @Override
                        public void flatMap(RawLivePos rawPositionData, Collector<RawLivePos> collector) throws Exception {
                            TmMetadata meta = Utils.getMetaData(rawPositionData.getVehicle().getVin(), config,"ud-enricher");
                            if(meta == null){
                                return;
                            }
                            rawPositionData.setTmMetadata(meta);
                            collector.collect(rawPositionData);
                        }
                    })
                    .sinkTo(output);
            env.execute(config.getFlinkJobName());
        }
    }
}
