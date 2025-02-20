package io.ballerina.stdlib.kafka.serializer;

import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.utils.ModuleUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;

public class KafkaAvroDeserializer implements Deserializer<Object> {
    BObject deserializer = null;
    public KafkaAvroDeserializer(BString schemaRegistryUrl) throws BError {
        try {
            this.deserializer = (schemaRegistryUrl != null) ? ValueCreator.createObjectValue(ModuleUtils.getModule(),
                    "Deserializer", schemaRegistryUrl) : null;
        } catch (Exception e) {
            this.deserializer = null;
        }
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        if (this.deserializer == null) {
            return createKafkaError("Deserializer is not found");
        }
        BArray value = ValueCreator.createArrayValue(data);
        Object[] arguments = new Object[]{value};
        return ModuleUtils.getEnvironment()
                .getRuntime().callMethod(this.deserializer, "deserialize", null, arguments);
    }

    @Override
    public Object deserialize(String topic, Headers headers, byte[] data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }

    @Override
    public Object deserialize(String topic, Headers headers, ByteBuffer data) {
        return Deserializer.super.deserialize(topic, headers, data);
    }
}
