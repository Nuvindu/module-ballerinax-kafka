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
