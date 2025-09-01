package za.co.trackmatic.flink.serde;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * A simple Kafka deserializer for Flink that converts Kafka byte array values
 * into UTF-8 encoded {@link String} objects.
 *
 * <p>This is useful when Kafka messages are plain strings encoded as bytes.
 */
public class StringDeserializer implements KafkaRecordDeserializationSchema<String> {
    /**
     * Deserializes a Kafka {@link ConsumerRecord} into a {@link String} and collects it.
     * <p>
     * If the value is not a byte array, null, or if the collector is null, the record is ignored.
     *
     * @param consumerRecord The Kafka record to deserialize.
     * @param collector The Flink collector to emit the deserialized string.
     * @throws IOException if deserialization fails (not expected in this implementation).
     */
    @Override
    public void deserialize(ConsumerRecord consumerRecord, Collector collector) throws IOException {
        if (collector == null || consumerRecord == null) {
            return;
        }

        if (consumerRecord.value() == null) {
            return;
        }

        if (!(consumerRecord.value() instanceof byte[])) {
            return;
        }

        byte[] b = (byte[]) consumerRecord.value();
        collector.collect(new String(b));
    }

    /**
     * Returns the produced type for this deserializer, which is {@link String}.
     *
     * @return {@link TypeInformation} of String.
     */
    @Override
    public TypeInformation<String> getProducedType() {
        return TypeInformation.of(String.class);
    }
}
