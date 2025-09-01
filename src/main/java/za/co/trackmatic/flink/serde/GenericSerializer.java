package za.co.trackmatic.flink.serde;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * A generic Kafka serializer for Flink that serializes objects of type {@code T}
 * into JSON byte arrays using Jackson and publishes them to a specified Kafka topic.
 *
 * @param <T> The type of object to serialize.
 */
public class GenericSerializer<T> implements KafkaRecordSerializationSchema<T> {
    final ObjectMapper om = new ObjectMapper();

    final String topic;

    /**
     * Constructs a {@link GenericSerializer} for the specified Kafka topic.
     *
     * @param topic The name of the Kafka topic to which records will be sent.
     */
    public GenericSerializer(String topic) {
        this.topic = topic;
    }

    /**
     * Initializes the serializer. Called once per parallel subtask before any records are serialized.
     *
     * @param context     The Flink {@link SerializationSchema.InitializationContext}.
     * @param sinkContext The Kafka sink context.
     * @throws Exception if initialization fails.
     */
    @Override
    public void open(SerializationSchema.InitializationContext context, KafkaSinkContext sinkContext) throws Exception {
        KafkaRecordSerializationSchema.super.open(context, sinkContext);
    }

    /**
     * Serializes an object of type {@code T} into a {@link ProducerRecord}.
     * The object is converted to a JSON byte array using Jackson.
     *
     * @param data            The data object to serialize.
     * @param kafkaSinkContext Context information for the Kafka sink.
     * @param aLong       The record timestamp (can be null).
     * @return A {@link ProducerRecord} containing the serialized bytes, or null if serialization fails.
     */
    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(T data, KafkaSinkContext kafkaSinkContext, Long aLong) {
        try {
            byte[] res = om.writeValueAsBytes(data);
            return new ProducerRecord<>(topic, res);
        } catch (Exception e) {
            return null;
        }
    }
}
