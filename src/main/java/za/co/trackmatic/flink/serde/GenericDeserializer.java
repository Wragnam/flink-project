package za.co.trackmatic.flink.serde;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * A generic Kafka deserializer for Flink that deserializes Kafka records into Java objects using Jackson.
 *
 * @param <T> The target type into which Kafka records should be deserialized.
 */
public class GenericDeserializer<T> implements KafkaRecordDeserializationSchema<T> {
    private static final long serialVersionUID = 1L;

    private ObjectMapper om = new ObjectMapper();

    private final Class<T> type;

    /**
     * Constructs a new {@link GenericDeserializer} for the given type.
     *
     * @param type The class type of the object to deserialize into.
     */
    public GenericDeserializer(Class<T> type) {
        this.type = type;
    }

    private static final String TAG = "GenericDeserializer";

    /**
     * Returns the type produced by this deserializer.
     *
     * @return {@link TypeInformation} representing the type {@code T}.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }

    /**
     * Deserializes a {@link ConsumerRecord} from Kafka into an object of type {@code T}
     * and collects it for downstream processing in Flink.
     *
     * <p>Logs any issues encountered during deserialization, such as null values
     * or unexpected types.
     *
     * @param consumerRecord The Kafka record to deserialize.
     * @param collector The Flink collector to output the deserialized object.
     * @throws IOException if deserialization fails due to invalid JSON or mapping issues.
     */
    @Override
    public void deserialize(ConsumerRecord consumerRecord, Collector collector) throws IOException {
        if (collector == null) {
            System.out.println("Collector is null. UNABLE TO PROCEED. FLINK ERROR?");
            return;
        }

        if (consumerRecord == null) {
            System.out.println("consumer record is null. Ignoring");
            return;
        }

        if (consumerRecord.key() != null) {
            System.out.println("key=" + consumerRecord.key() + " - " + consumerRecord.key().getClass().getName());
        }

        if (consumerRecord.value() != null) {
            if (!(consumerRecord.value() instanceof byte[])) {
                return;
            }

            byte[] b = (byte[]) consumerRecord.value();
            //System.out.println("value=" + new String(b) + " - " + consumerRecord.value().getClass().getName());

            T raw = om.readValue(b, type);
            if (raw == null) {
                System.out.println("Error deserializing. Orig: " + new String(b));
                return;
            }
            collector.collect(raw);
        } else {
            System.out.println("value is null");
        }
    }

}
