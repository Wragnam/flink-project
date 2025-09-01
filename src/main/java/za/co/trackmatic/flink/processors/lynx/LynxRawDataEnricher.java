package za.co.trackmatic.flink.processors.lynx;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.lynx.LynxRawData;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericListDeserializer;
import za.co.trackmatic.flink.serde.GenericSerializer;

/**
 * Flink processor to enrich raw Lynx telemetry data with additional metadata
 * and forward the enriched data to multiple Kafka topics.
 */
public class LynxRawDataEnricher {

    /**
     * Constructor for LynxRawDataEnricher.
     * This class is responsible for enriching raw Lynx data and then sinking it to Kafka.
     */
    public LynxRawDataEnricher(){}

    /**
     * Processes the raw Lynx data by enriching it with metadata and sinks the enriched data to Kafka.
     *
     * @param config the configuration for Kafka source and sink, along with other processing options
     * @throws Exception if any error occurs during processing
     */
    public void process(Config config) throws Exception{
        // Create Flink execution environment for stream processing
        try(StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()){
            // Set generic options for the Flink environment
            Utils.setGenericDataStreamOptions(env,60000,10000);

            // Create Kafka source for Raw Data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<LynxRawData> deserializer = new GenericListDeserializer<>(LynxRawData.class);
            KafkaSource<LynxRawData> rawInput = Utils.createKafkaSource(source,deserializer);

            // Kafka sink configuration for two different topics
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            String[] sinkTopic = sink.getTopics().split(",",2);

            // Create a sink for the first Kafka topic (Event data)
            GenericSerializer<LynxRawData> serializerEvent = new GenericSerializer<>(sinkTopic[0]);
            KafkaSink<LynxRawData> outputEvent = Utils.createKafkaSink(sink,serializerEvent);

            // Create a sink for the second Kafka topic (Position data)
            GenericSerializer<LynxRawData> serializerPosition = new GenericSerializer<>(sinkTopic[1]);
            KafkaSink<LynxRawData> outputPosition = Utils.createKafkaSink(sink, serializerPosition);

            // Process the raw input stream: Enrich it with metadata and other information
            DataStream<LynxRawData> processedStream = env.fromSource(rawInput, WatermarkStrategy.noWatermarks(),"lynx:raw->enriched").flatMap(new FlatMapFunction<LynxRawData, LynxRawData>() {
                /**
                 * Enriches raw Lynx data by adding metadata and logging the process.
                 *
                 * @param lynxRawData the raw data to be enriched
                 * @param collector collects the enriched LynxRawData for further processing
                 * @throws Exception if an error occurs during processing
                 */
                @Override
                public void flatMap(LynxRawData lynxRawData, Collector<LynxRawData> collector) throws Exception {
                    // Clean the asset serial number from raw data
                    String serial = Utils.cleanSerial(lynxRawData.getAssetInfo().getAssetId());
                    TmMetadata metadata = Utils.getMetaData(serial,config,"lynx-enricher");

                    // If metadata is null, skip processing the raw data
                    if(metadata == null){
                        return;
                    }

                    // Set the metadata on the raw data
                    lynxRawData.setMetadata(metadata);

                    // Collect the enriched data for further processing or sinking
                    collector.collect(lynxRawData);
                }
            });

            // Sink the enriched data to Kafka in the event topic
            processedStream.sinkTo(outputEvent);

            // Sink the enriched data to Kafka in the position topic
            processedStream.sinkTo(outputPosition);

            // Execute the Flink job with the configured job name
            env.execute(config.getFlinkJobName());
        }
    }
}
