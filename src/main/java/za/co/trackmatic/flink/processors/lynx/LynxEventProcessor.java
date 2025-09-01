package za.co.trackmatic.flink.processors.lynx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.events.GeneralEvent;
import za.co.trackmatic.flink.flatmaps.multipletps.UniversalLiveposEventProcessor;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.MultipleProviders.TripAndEventMappers;
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.serde.GenericDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * This class processes raw Lynx data events and enriches them to create general Lynx events,
 * outputting the enriched events to Kafka.
 */
public class LynxEventProcessor {

    /**
     * Default constructor for LynxEventProcessor.
     */
    public LynxEventProcessor(){}


    /**
     * Processes raw Lynx data events, applies enrichment logic, and outputs the results as general Lynx events to Kafka.
     *
     * @param config the configuration containing Kafka source and sink information, and other settings
     * @throws Exception if an error occurs during processing or execution
     */
    public void process(Config config) throws Exception{
        // Initialize Flink's StreamExecutionEnvironment
        try(StreamExecutionEnvironment env =StreamExecutionEnvironment.getExecutionEnvironment()){
            // Set Flink stream options
            Utils.setGenericDataStreamOptions(env, 60000,10000);

            // Create Kafka source for raw Lynx data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericDeserializer<LynxRawData> deserializer = new GenericDeserializer<>(LynxRawData.class);
            KafkaSource<LynxRawData> input = Utils.createKafkaSource(source,deserializer);

            // Create Kafka sink for enriched Lynx events
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<GeneralEvent> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<GeneralEvent> output = Utils.createKafkaSink(sink, serializer);

            // Process the raw Lynx data by keying by asset ID, applying the flatMap logic for processing
            env.fromSource(input, WatermarkStrategy.noWatermarks(), "lynx:enriched->event")
                    .map(TripAndEventMappers::fromLynxForEvent)
                    .keyBy(rawData -> rawData.getMetadata().getAssetId()) // Keying by asset ID
                    .flatMap(new UniversalLiveposEventProcessor(config)) // Applying the enrichment logic using a custom flatMap function
                    // Sink the enriched events to Kafka
                    .sinkTo(output);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
