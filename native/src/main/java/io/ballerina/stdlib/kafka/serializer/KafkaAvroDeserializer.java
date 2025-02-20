/*
 * Copyright (c) 2025, WSO2 LLC. (http://www.wso2.com)
 *
 * WSO2 LLC. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.ballerina.stdlib.kafka.serializer;

import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.api.values.BTypedesc;
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
                    "KafkaAvroDeserializer", schemaRegistryUrl) : null;
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
