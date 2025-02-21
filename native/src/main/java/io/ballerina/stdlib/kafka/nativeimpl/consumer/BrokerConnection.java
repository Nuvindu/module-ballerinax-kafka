/*
 * Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
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

package io.ballerina.stdlib.kafka.nativeimpl.consumer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BDecimal;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.serializer.KafkaAvroDeserializer;
import io.ballerina.stdlib.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Properties;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.ALIAS_DURATION;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.BOOTSTRAP_SERVERS;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_BOOTSTRAP_SERVERS_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.CONSUMER_CONFIG_FIELD_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.DURATION_UNDEFINED_VALUE;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.KAFKA_SERVERS;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_CONSUMER_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.UNCHECKED;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getClientIdFromProperties;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getDefaultApiTimeout;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getIntFromBDecimal;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getServerUrls;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionList;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.processKafkaConsumerConfig;

/**
 * Native methods to handle the connection between Ballerina Kafka Consumer and the Kafka Broker.
 */
public class BrokerConnection {

    private static final Logger logger = LoggerFactory.getLogger(BrokerConnection.class);

    /**
     * Closes the connection between ballerina kafka consumer and the kafka broker.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @param duration       Duration in milliseconds to try the operation.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object close(Environment environment, BObject consumerObject, BDecimal duration) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        Properties consumerProperties = (Properties) consumerObject.getNativeData(NATIVE_CONSUMER_CONFIG);
        int defaultApiTimeout = getDefaultApiTimeout(consumerProperties);
        int apiTimeout = getIntFromBDecimal(duration, logger, ALIAS_DURATION);
        try {
            synchronized (kafkaConsumer) {
                // API timeout should given the priority over the default value
                if (apiTimeout > DURATION_UNDEFINED_VALUE) {
                    closeWithDuration(kafkaConsumer, apiTimeout);
                } else if (defaultApiTimeout > DURATION_UNDEFINED_VALUE) {
                    closeWithDuration(kafkaConsumer, defaultApiTimeout);
                } else {
                    kafkaConsumer.close();
                }
            }
            KafkaMetricsUtil.reportConsumerClose(consumerObject);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_CLOSE);
            return createKafkaError("Failed to close the connection from Kafka server: " + e.getMessage());
        }
        return null;
    }

    /**
     * Connects ballerina kafka consumer to a kafka broker.
     *
     * @param consumerObject Kafka consumer object from ballerina.
     * @return {@code BError}, if there's an error, null otherwise.
     */
    @SuppressWarnings(UNCHECKED)
    public static Object connect(BObject consumerObject) {
        // Check whether already native consumer is attached to the struct.
        // This can be happen either from Kafka service or via programmatically.
        if (Objects.nonNull(consumerObject.getNativeData(NATIVE_CONSUMER))) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_CONNECTION);
            return createKafkaError(
                    "Kafka consumer is already connected to external broker. Please close it before re-connecting " +
                            "the external broker again.");
        }
        Object bootStrapServers = consumerObject.get(CONSUMER_BOOTSTRAP_SERVERS_CONFIG);
        BMap<BString, Object> configs = consumerObject.getMapValue(CONSUMER_CONFIG_FIELD_NAME);
        Properties consumerProperties = processKafkaConsumerConfig(bootStrapServers, configs);
        BError keyDeserialization = addKeyDeserializerConfig(consumerProperties, configs);
        BError valueDeserialization = addValueDeserializerConfig(consumerProperties, configs);
        if (keyDeserialization != null) {
            return keyDeserialization;
        }
        if (valueDeserialization != null) {
            return valueDeserialization;
        }
        try {
            KafkaConsumer kafkaConsumer;
            if ((consumerProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    instanceof KafkaAvroDeserializer keyDeserializer)
                    && (consumerProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    instanceof KafkaAvroDeserializer valueDeserializer)) {
                kafkaConsumer = new KafkaConsumer<>(consumerProperties, keyDeserializer, valueDeserializer);
            } else if (consumerProperties.get(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG)
                    instanceof KafkaAvroDeserializer keyDeserializer) {
                kafkaConsumer = new KafkaConsumer<>(consumerProperties, keyDeserializer, null);
            } else if (consumerProperties.get(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG)
                    instanceof KafkaAvroDeserializer valueDeserializer) {
                kafkaConsumer = new KafkaConsumer<>(consumerProperties, null, valueDeserializer);
            } else {
                kafkaConsumer = new KafkaConsumer<>(consumerProperties);
            }
            consumerObject.addNativeData(NATIVE_CONSUMER, kafkaConsumer);
            consumerObject.addNativeData(NATIVE_CONSUMER_CONFIG, consumerProperties);
            consumerObject.addNativeData(BOOTSTRAP_SERVERS, consumerProperties.getProperty(BOOTSTRAP_SERVERS));
            consumerObject.addNativeData(KafkaConstants.CLIENT_ID, getClientIdFromProperties(consumerProperties));
            KafkaMetricsUtil.reportNewConsumer(consumerObject);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_CONNECTION);
            return createKafkaError("Cannot connect to the kafka server: " + e.getMessage());
        }
        if (logger.isDebugEnabled()) {
            logger.debug(KAFKA_SERVERS + getServerUrls(bootStrapServers));
        }
        return null;
    }

    public static BError addKeyDeserializerConfig(Properties properties, BMap<BString, Object> configs) {
        if (configs.get(KafkaConstants.KEY_DESERIALIZER_TYPE)
                .equals(StringUtils.fromString(KafkaConstants.AVRO_DESERIALIZER))) {
            if (!configs.containsKey(KafkaConstants.SCHEMA_REGISTRY_CONFIG)) {
                return createKafkaError("Schema Registry configs are required for Avro deserialization");
            }
            BMap<BString, Object> registryConfig =
                    (BMap<BString, Object>) configs.get(KafkaConstants.SCHEMA_REGISTRY_CONFIG);
            BString baseUrl = (BString) registryConfig.get(KafkaConstants.SCHEMA_REGISTRY_BASE_URL);
            BMap<BString, Object> originals = (BMap<BString, Object>) registryConfig
                    .getMapValue(KafkaConstants.SCHEMA_REGISTRY_ORIGINALS);
            BMap<BString, Object> httpHeaders = (BMap<BString, Object>) registryConfig
                    .getMapValue(KafkaConstants.SCHEMA_REGISTRY_HEADERS);
            KafkaAvroDeserializer deserializer;
            try {
                deserializer = new KafkaAvroDeserializer(baseUrl, originals, httpHeaders);
            } catch (BError e) {
                return createKafkaError(e.getMessage());
            }
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        } else {
            properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.BYTE_ARRAY_DESERIALIZER);
        }
        return null;
    }

    public static BError addValueDeserializerConfig(Properties properties, BMap<BString, Object> configs) {
        if (configs.get(KafkaConstants.VALUE_DESERIALIZER_TYPE)
                .equals(StringUtils.fromString(KafkaConstants.AVRO_DESERIALIZER))) {
            if (!configs.containsKey(KafkaConstants.SCHEMA_REGISTRY_CONFIG)) {
                return createKafkaError("Schema Registry configs are required for Avro deserialization");
            }
            BMap<BString, Object> registryConfig =
                    (BMap<BString, Object>) configs.get(KafkaConstants.SCHEMA_REGISTRY_CONFIG);
            BString baseUrl = (BString) registryConfig.get(KafkaConstants.SCHEMA_REGISTRY_BASE_URL);
            BMap<BString, Object> originals = (BMap<BString, Object>) registryConfig
                    .getMapValue(KafkaConstants.SCHEMA_REGISTRY_ORIGINALS);
            BMap<BString, Object> httpHeaders = (BMap<BString, Object>) registryConfig
                    .getMapValue(KafkaConstants.SCHEMA_REGISTRY_HEADERS);
            KafkaAvroDeserializer deserializer;
            try {
                deserializer = new KafkaAvroDeserializer(baseUrl, originals, httpHeaders);
            } catch (BError e) {
                return createKafkaError(e.getMessage());
            }
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        } else {
            properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaConstants.BYTE_ARRAY_DESERIALIZER);
        }
        return null;
    }

    /**
     * Pauses ballerina kafka consumer connection with the kafka broker.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic Partitions which needed to be paused.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object pause(Environment environment, BObject consumerObject, BArray topicPartitions) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        ArrayList<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);

        try {
            kafkaConsumer.pause(partitionList);
        } catch (IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_PAUSE);
            return createKafkaError("Failed to pause topic partitions for the consumer: " + e.getMessage());
        }
        return null;
    }

    /**
     * Resumes a paused connection between ballerina kafka consumer and kafka broker.
     *
     * @param consumerObject  Kafka consumer object from ballerina.
     * @param topicPartitions Topic Partitions which are currently paused and needed to be resumed.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object resume(Environment environment, BObject consumerObject, BArray topicPartitions) {
        KafkaTracingUtil.traceResourceInvocation(environment, consumerObject);
        KafkaConsumer kafkaConsumer = (KafkaConsumer) consumerObject.getNativeData(NATIVE_CONSUMER);
        ArrayList<TopicPartition> partitionList = getTopicPartitionList(topicPartitions, logger);

        try {
            synchronized (kafkaConsumer) {
                kafkaConsumer.resume(partitionList);
            }
        } catch (IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportConsumerError(consumerObject, KafkaObservabilityConstants.ERROR_TYPE_RESUME);
            return createKafkaError("Failed to resume topic partitions for the consumer: " + e.getMessage());
        }
        return null;
    }

    private static void closeWithDuration(KafkaConsumer kafkaConsumer, long timeout) {
        Duration duration = Duration.ofMillis(timeout);
        kafkaConsumer.close(duration);
    }
}
