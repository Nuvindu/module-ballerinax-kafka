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

package io.ballerina.stdlib.kafka.nativeimpl.producer;

import io.ballerina.runtime.api.Environment;
import io.ballerina.runtime.api.creators.TypeCreator;
import io.ballerina.runtime.api.creators.ValueCreator;
import io.ballerina.runtime.api.utils.StringUtils;
import io.ballerina.runtime.api.values.BArray;
import io.ballerina.runtime.api.values.BError;
import io.ballerina.runtime.api.values.BMap;
import io.ballerina.runtime.api.values.BObject;
import io.ballerina.runtime.api.values.BString;
import io.ballerina.runtime.transactions.TransactionResourceManager;
import io.ballerina.stdlib.kafka.impl.KafkaTransactionContext;
import io.ballerina.stdlib.kafka.observability.KafkaMetricsUtil;
import io.ballerina.stdlib.kafka.observability.KafkaObservabilityConstants;
import io.ballerina.stdlib.kafka.observability.KafkaTracingUtil;
import io.ballerina.stdlib.kafka.serializer.KafkaAvroSerializer;
import io.ballerina.stdlib.kafka.utils.KafkaConstants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static io.ballerina.stdlib.kafka.utils.KafkaConstants.NATIVE_PRODUCER;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PRODUCER_BOOTSTRAP_SERVERS_CONFIG;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.PRODUCER_CONFIG_FIELD_NAME;
import static io.ballerina.stdlib.kafka.utils.KafkaConstants.TRANSACTION_CONTEXT;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaError;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.createKafkaProducer;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.getTopicPartitionRecord;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.populateTopicPartitionRecord;
import static io.ballerina.stdlib.kafka.utils.KafkaUtils.processKafkaProducerConfig;
import static io.ballerina.stdlib.kafka.utils.TransactionUtils.createKafkaTransactionContext;
import static io.ballerina.stdlib.kafka.utils.TransactionUtils.handleTransactions;

/**
 * Native methods to handle ballerina kafka producer.
 */
public class ProducerActions {

    /**
     * Initializes the ballerina kafka producer.
     *
     * @param producerObject Kafka producer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object init(BObject producerObject) {
        Object bootstrapServer = producerObject.get(PRODUCER_BOOTSTRAP_SERVERS_CONFIG);
        BMap<BString, Object> configs = producerObject.getMapValue(PRODUCER_CONFIG_FIELD_NAME);
        Properties producerProperties = processKafkaProducerConfig(bootstrapServer, configs);
        BError keySerialization = addKeySerializerConfig(producerProperties, configs);
        BError valueSerialization = addValueSerializerConfig(producerProperties, configs);
        if (keySerialization != null) {
            return keySerialization;
        }
        if (valueSerialization != null) {
            return valueSerialization;
        }
        try {
            if (Objects.nonNull(producerProperties.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG))) {
                if (!((boolean) producerProperties.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG))) {
                    return createKafkaError("configuration enableIdempotence must be set to true to enable " +
                                                            "transactional producer");
                }
                createKafkaProducer(producerProperties, producerObject);
                KafkaTransactionContext transactionContext = createKafkaTransactionContext(producerObject);
                producerObject.addNativeData(TRANSACTION_CONTEXT, transactionContext);
            } else {
                createKafkaProducer(producerProperties, producerObject);
            }
        } catch (IllegalStateException | KafkaException e) {
            KafkaMetricsUtil.reportProducerError(producerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_CONNECTION);
            return createKafkaError("Failed to initialize the producer: " + e.getCause().getMessage());
        }
        return null;
    }

    public static BError addKeySerializerConfig(Properties properties, BMap<BString, Object> configs) {
        if (configs.get(KafkaConstants.KEY_SERIALIZER_TYPE)
                .equals(StringUtils.fromString(KafkaConstants.AVRO_SERIALIZER))) {
            if (!configs.containsKey(KafkaConstants.PRODUCER_SCHEMA_REGISTRY_URL)) {
                return createKafkaError("Schema Registry is required for Avro serialization");
            }
            if (!configs.containsKey(KafkaConstants.PRODUCER_AVRO_SCHEMA)) {
                return createKafkaError("Schema is required for Avro serialization");
            }
            BString schemaRegistryUrl = (BString) configs.get(KafkaConstants.PRODUCER_SCHEMA_REGISTRY_URL);
            BString avroSchema = (BString) configs.get(KafkaConstants.PRODUCER_AVRO_SCHEMA);
            KafkaAvroSerializer serializer;
            try {
                serializer = new KafkaAvroSerializer(schemaRegistryUrl, avroSchema);
            } catch (BError e) {
                return createKafkaError(e.getMessage());
            }
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        } else {
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstants.BYTE_ARRAY_SERIALIZER);
        }
        return null;
    }

    public static BError addValueSerializerConfig(Properties properties, BMap<BString, Object> configs) {
        if (configs.get(KafkaConstants.VALUE_SERIALIZER_TYPE)
                .equals(StringUtils.fromString(KafkaConstants.AVRO_SERIALIZER))) {
            if (!configs.containsKey(KafkaConstants.PRODUCER_SCHEMA_REGISTRY_URL)) {
                return createKafkaError("Schema Registry is required for Avro serialization");
            }
            if (!configs.containsKey(KafkaConstants.CONSUMER_AVRO_SCHEMA)) {
                return createKafkaError("Schema is required for Avro serialization");
            }
            BString schemaRegistryUrl = (BString) configs.get(KafkaConstants.CONSUMER_SCHEMA_REGISTRY_URL);
            BString avroSchema = (BString) configs.get(KafkaConstants.CONSUMER_AVRO_SCHEMA);
            KafkaAvroSerializer serializer;
            try {
                serializer = new KafkaAvroSerializer(schemaRegistryUrl, avroSchema);
            } catch (BError e) {
                return createKafkaError(e.getMessage());
            }
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);
        } else {
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConstants.BYTE_ARRAY_SERIALIZER);
        }
        return null;
    }

    /**
     * Closes the connection between ballerina kafka producer and the kafka broker.
     *
     * @param producerObject Kafka producer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object close(Environment environment, BObject producerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, producerObject);
        KafkaProducer kafkaProducer = (KafkaProducer) producerObject.getNativeData(NATIVE_PRODUCER);
        try {
            kafkaProducer.close();
            KafkaMetricsUtil.reportProducerClose(producerObject);
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportProducerError(producerObject, KafkaObservabilityConstants.ERROR_TYPE_CLOSE);
            return createKafkaError("Failed to close the Kafka producer: " + e.getMessage());
        }
        return null;
    }

    /**
     * Makes all the records buffered are immediately available.
     *
     * @param producerObject Kafka producer object from ballerina.
     * @return {@code BError}, if there's any error, null otherwise.
     */
    public static Object flushRecords(Environment environment, BObject producerObject) {
        KafkaTracingUtil.traceResourceInvocation(environment, producerObject);
        KafkaProducer kafkaProducer = (KafkaProducer) producerObject.getNativeData(NATIVE_PRODUCER);
        try {
            if (TransactionResourceManager.getInstance().isInTransaction()) {
                handleTransactions(producerObject);
            }
            kafkaProducer.flush();
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportProducerError(producerObject, KafkaObservabilityConstants.ERROR_TYPE_FLUSH);
            return createKafkaError("Failed to flush Kafka records: " + e.getMessage());
        }
        return null;
    }

    /**
     * Get information about a given topic.
     *
     * @param producerObject Kafka producer object from ballerina.
     * @param topic Topic about which the information is needed.
     * @return Ballerina {@code TopicPartition[]} for the given topic.
     */
    public static Object getTopicPartitions(Environment environment, BObject producerObject, BString topic) {
        KafkaTracingUtil.traceResourceInvocation(environment, producerObject, topic.getValue());
        KafkaProducer kafkaProducer = (KafkaProducer) producerObject.getNativeData(NATIVE_PRODUCER);
        try {
            if (TransactionResourceManager.getInstance().isInTransaction()) {
                handleTransactions(producerObject);
            }
            List<PartitionInfo> partitionInfoList = kafkaProducer.partitionsFor(topic.getValue());
            BArray topicPartitionArray =
                    ValueCreator.createArrayValue(TypeCreator.createArrayType(getTopicPartitionRecord().getType()));
            for (PartitionInfo info : partitionInfoList) {
                BMap<BString, Object> partition = populateTopicPartitionRecord(info.topic(), info.partition());
                topicPartitionArray.append(partition);
            }
            return topicPartitionArray;
        } catch (KafkaException e) {
            KafkaMetricsUtil.reportProducerError(producerObject,
                                                 KafkaObservabilityConstants.ERROR_TYPE_TOPIC_PARTITIONS);
            return createKafkaError("Failed to fetch partitions from the producer " + e.getMessage());
        }
    }
}
