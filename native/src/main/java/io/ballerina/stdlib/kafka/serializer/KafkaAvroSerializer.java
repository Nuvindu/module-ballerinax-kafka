package io.ballerina.stdlib.kafka.serializer;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class KafkaAvroSerializer implements Serializer<Object> {
    BObject serializer = null;
    public KafkaAvroSerializer(BString schemaRegistryUrl, BString avroSchema) {
        try {
            this.serializer = (schemaRegistryUrl != null && avroSchema != null)
                    ? ValueCreator.createObjectValue(ModuleUtils.getModule(), "Serializer",
                    schemaRegistryUrl, avroSchema) : null;
        } catch (BError e) {
            this.serializer = null;
        }

    }

    public void configure(Map<String, ?> configs) {

    }

    public byte[] serialize(String topic, Object value) {
        if (this.serializer == null) {
            throw new RuntimeException("Serializer not found");
        }
        BString kafkaTopic = StringUtils.fromString(topic);
        Object[] arguments = new Object[]{kafkaTopic, value};
        BArray result = (BArray) ModuleUtils.getEnvironment()
                .getRuntime().callMethod(this.serializer, "serialize", null, arguments);
        return result.getByteArray();
    }

    public void close() {
    }
}
