package za.co.trackmatic.flink.processors.loads;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.flatmaps.loads.ExtrapolateProcessing;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.loads.extrapolation.ExtrapolationReqAndRespEvent;
import za.co.trackmatic.flink.models.loads.extrapolation.ParagonLivePosData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * This class processes live position data and extrapolation request/response events,
 * applying extrapolation logic and outputting the results as Kafka events.
 */
public class LoadsUpdateStopProcessor {
    /**
     * Default constructor for LoadsUpdateStopProcessor.
     */
    public LoadsUpdateStopProcessor(){}

    /**
     * Processes live position data and extrapolation events, applies extrapolation logic,
     * and outputs the results to Kafka as extrapolation request/response events.
     *
     * @param config the configuration containing Kafka source and sink information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception {
        // Initialize Flink's StreamExecutionEnvironment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000, 1000);
            // Set parallelism for the job
            env.setParallelism(2);

            // Create Kafka source for live position data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<ParagonLivePosData> deserializerLivePos = new GenericDeserializer<>(ParagonLivePosData.class);
            KafkaSource<ParagonLivePosData> sourcePos = Utils.createKafkaSource(0,source, deserializerLivePos);

            // Create Kafka source for extrapolation request/response events
            GenericDeserializer<ExtrapolationReqAndRespEvent> deserializerNextStop = new GenericDeserializer<>(ExtrapolationReqAndRespEvent.class);
            KafkaSource<ExtrapolationReqAndRespEvent> sourceNextStop = Utils.createKafkaSource(1,source, deserializerNextStop);

            // Create Kafka sink for extrapolation request/response events
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<ExtrapolationReqAndRespEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<ExtrapolationReqAndRespEvent> output = Utils.createKafkaSink(sink, serializer);

            // Connect the live position data and extrapolation request/response events
            ConnectedStreams<ParagonLivePosData, ExtrapolationReqAndRespEvent> information = env.fromSource(sourcePos, WatermarkStrategy.noWatermarks(), "paragon:livepos->processed")
                            .connect(env.fromSource(sourceNextStop, WatermarkStrategy.noWatermarks(), "nextStopInfo->processedNextStopInfo"));

            // Apply the extrapolation processing logic and sink the output to Kafka
            information.keyBy(ParagonLivePosData::getAssetId, value -> value.getPayload().getAssetId())
                            .flatMap(new ExtrapolateProcessing(config)).sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }

}
