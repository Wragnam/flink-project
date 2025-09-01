package za.co.trackmatic.flink.processors.rms;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import za.co.trackmatic.flink.Utils.Utils;
import za.co.trackmatic.flink.models.Config;
import za.co.trackmatic.flink.models.rms.RmsLiveposRaw;
import za.co.trackmatic.flink.models.trackmatic.TmMetadata;
import za.co.trackmatic.flink.serde.GenericSerializer;
import za.co.trackmatic.flink.serde.GenericListDeserializer;

import java.io.Serializable;

/**
 * Flink processor to enrich raw Rms position events with metadata.
 *
 * <p>This processor consumes {@link RmsLiveposRaw} events from Kafka, enriches each event by
 * attaching Trackmatic metadata based on the asset ID, and produces the enriched events back
 * to Kafka for downstream processing.</p>
 */
public class RmsLiveposEnricher implements Serializable {

    /**
     * Default constructor for {@code RmsLiveposEnricher}.
     */
    public RmsLiveposEnricher() {}

    /**
     * Main method to process raw position events and enrich them with metadata.
     * It reads RmsLiveposRaw data from Kafka, enriches it with metadata, and writes the enriched events to Kafka.
     *
     * @param config The configuration object containing Kafka source and sink details.
     * @throws Exception If an error occurs during the Flink job execution.
     */
    public void process(Config config) throws Exception {
        // Create a Flink execution environment
        try (StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()) {
            // Set data stream options
            Utils.setGenericDataStreamOptions(env, 60000, 10000);

            // Configure Kafka source and deserializer for RmsLiveposRaw data
            Config.KafkaSourceSink source = config.getKafka().getSource();
            GenericListDeserializer<RmsLiveposRaw> deserializer = new GenericListDeserializer<>(RmsLiveposRaw.class);
            KafkaSource<RmsLiveposRaw> rawInput = Utils.createKafkaSource(source, deserializer);

            // Configure Kafka sink and serializer for RmsLiveposRaw data
            Config.KafkaSourceSink sink = config.getKafka().getSink();
            GenericSerializer<RmsLiveposRaw> serializer = new GenericSerializer<>(sink.getTopics());
            KafkaSink<RmsLiveposRaw> output = Utils.createKafkaSink(source, serializer);

            // Process the raw RmsLiveposRaw data stream
            env.fromSource(rawInput, WatermarkStrategy.noWatermarks(), "rms:livepos-raw->event-enriched").flatMap(new FlatMapFunction<RmsLiveposRaw, RmsLiveposRaw>() {
                /**
                 * Enriches each `RmsLiveposRaw` event by fetching metadata based on the assetId.
                 * If metadata is found, it is added to the event; otherwise, the event is skipped.
                 *
                 * @param rmsLiveposRaw The raw position event to be enriched.
                 * @param collector The collector used to emit the enriched event downstream.
                 * @throws Exception If any error occurs during the enrichment process.
                 */
                @Override
                public void flatMap(RmsLiveposRaw rmsLiveposRaw, Collector<RmsLiveposRaw> collector) throws Exception {
                    // Clean the assetId and retrieve metadata associated with it
                    String serial = Utils.cleanSerial(String.valueOf(rmsLiveposRaw.getVehiclePk()));
                    TmMetadata metadata = Utils.getMetaData(serial, config, "rms-livepos-enricher");

                    // If no metadata is found, skip the event
                    if (metadata == null) {
                        return;
                    }

                    // Set the metadata to the raw event
                    rmsLiveposRaw.setMetadata(metadata);

                    // Emit the enriched event downstream
                    collector.collect(rmsLiveposRaw);
                }
            }).sinkTo(output);// Write the enriched event to Kafka sink

            // Execute the Flink job with the provided job name from the config
            env.execute(config.getFlinkJobName());
        }
    }
}

