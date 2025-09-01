package za.co.trackmatic.flink.serde;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import za.co.trackmatic.flink.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * A generic Kafka deserializer for Flink that converts a JSON-encoded list of objects
 * into individual elements of type {@code T} using Jackson.
 * <p>
 * This is useful when Kafka messages contain a JSON array rather than a single object.
 *
 * @param <T> The type of elements in the JSON list.
 */
public class GenericListDeserializer<T> implements KafkaRecordDeserializationSchema<T>, Serializable {
    private transient ObjectMapper objectMapper;
    private final Class<T> tClass;

    /**
     * Returns the type produced by this deserializer.
     *
     * @return {@link TypeInformation} of type {@code T}.
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return Types.POJO(tClass);
    }

    /**
     * Constructs a {@link GenericListDeserializer} with the specified class type.
     *
     * @param tClass The class type of the list elements.
     */
    public GenericListDeserializer(Class<T> tClass) {
        this.tClass = tClass;
    }

    /**
     * Initializes the deserializer. Called once per parallel subtask before any records are deserialized.
     *
     * @param context The {@link DeserializationSchema.InitializationContext} containing runtime context and metrics.
     */
    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Deserializes a {@link ConsumerRecord} containing a JSON array into a list of objects of type {@code T}.
     * Each item in the list is emitted individually to the Flink pipeline.
     *
     * @param consumerRecord The raw Kafka record.
     * @param collector      The output collector to emit deserialized items.
     * @throws IOException if deserialization fails.
     */
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<T> collector) throws IOException {
        if(objectMapper == null){
            objectMapper = new ObjectMapper();
        }

        byte[] value = consumerRecord.value();
        if (value != null) {
            try {
                // Use Jackson's TypeFactory to handle generic list deserialization
                CollectionType javaType = objectMapper.getTypeFactory().constructCollectionType(List.class, tClass);
                List<T> dataList = objectMapper.readValue(value, javaType);

                if(dataList == null){
                    Logger.log("Received null value in ConsumerRecord, returning deserialization.", 5600);
                    return;
                }

                // Collect each item in the list individually
                for (T dataItem : dataList) {
                    collector.collect(dataItem);
                }
            } catch (Exception e) {
                Logger.log("Error deserializing the record: " + e.getMessage(), 5600);
                e.printStackTrace();
                throw new IOException("Error deserializing the record", e);  // Forward the exception to indicate failure
            }
        } else {
            Logger.log("Received null value in ConsumerRecord, skipping deserialization.", 5600);
        }
    }
}
