/*
 * Copyright (c) 2021, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.ballerina.stdlib.kafka.plugin;

/**
 * Kafka compiler plugin constants.
 */
public class PluginConstants {
    // compiler plugin constants
    public static final String PACKAGE_PREFIX = "kafka";
    public static final String REMOTE_QUALIFIER = "REMOTE";
    public static final String ON_RECORDS_FUNC = "onConsumerRecord";
    public static final String ON_ERROR_FUNC = "onError";
    public static final String PACKAGE_ORG = "ballerinax";

    // parameters
    public static final String CALLER = "Caller";
    public static final String RECORD_PARAM = "ConsumerRecord";
    public static final String ERROR_PARAM = "Error";
    public static final String PAYLOAD_ANNOTATION = "kafka:Payload ";

    // consumer record fields
    public static final String CONSUMER_RECORD_VALUE = "value";
    public static final String CONSUMER_RECORD_KEY = "key";
    public static final String CONSUMER_RECORD_TIMESTAMP = "timestamp";
    public static final String CONSUMER_RECORD_OFFSET = "offset";
    public static final String CONSUMER_RECORD_TOPIC = "topic";
    public static final String CONSUMER_RECORD_PARTITION = "partition";

    // return types error or nil
    public static final String ERROR = "error";

    // Code template related constants
    public static final String NODE_LOCATION = "node.location";
    public static final String LS = System.lineSeparator();
    public static final String CODE_TEMPLATE_NAME_WITH_CALLER = "ADD_REMOTE_FUNCTION_CODE_SNIPPET_WITH_CALLER";
    public static final String CODE_TEMPLATE_NAME_WITHOUT_CALLER = "ADD_REMOTE_FUNCTION_CODE_SNIPPET_WITHOUT_CALLER";

    /**
     * Compilation errors.
     */
    enum CompilationErrors {
        NO_ON_CONSUMER_RECORD("Service must have remote method onConsumerRecord.",
                "KAFKA_101"),
        INVALID_REMOTE_FUNCTION("Invalid remote method.", "KAFKA_102"),
        INVALID_RESOURCE_FUNCTION("Resource functions not allowed.", "KAFKA_103"),
        FUNCTION_SHOULD_BE_REMOTE("Method must have the remote qualifier.", "KAFKA_104"),
        MUST_HAVE_CALLER_AND_RECORDS("Must have the required parameter kafka:AnydataConsumerRecord[] or " +
                "anydata[] and optional parameter kafka:Caller.", "KAFKA_105"),
        INVALID_PARAM_TYPES("Invalid method parameters. Only subtypes of kafka:AnydataConsumerRecord[], " +
                "subtypes of anydata[] and kafka:Caller is allowed", "KAFKA_106"),
        INVALID_SINGLE_PARAMETER("Invalid method parameter. Only subtypes of kafka:AnydataConsumerRecord[]" +
                " or subtypes of anydata[] is allowed.", "KAFKA_107"),
        INVALID_PARAM_COUNT("Invalid method parameter count. " +
                "Only kafka:Caller and subtypes of kafka:AnydataConsumerRecord[] are allowed.", "KAFKA_108"),
        INVALID_RETURN_TYPE_ERROR_OR_NIL("Invalid return type. Only error? or kafka:Error? is allowed.",
                "KAFKA_109"),
        INVALID_MULTIPLE_LISTENERS("Multiple listener attachments. Only one kafka:Listener is allowed.",
                "KAFKA_110"),
        TEMPLATE_CODE_GENERATION_HINT("Template generation for empty service", "KAFKA_111"),
        MUST_HAVE_ERROR("Must have the required parameter kafka:Error", "KAFKA_112"),
        ONLY_ERROR_ALLOWED("Invalid method parameter. Only kafka:Error or error is allowed", "KAFKA_113"),
        ONLY_CALLER_ALLOWED("Invalid method parameter. Only kafka:Caller is allowed", "KAFKA_114");

        private final String error;
        private final String errorCode;

        CompilationErrors(String error, String errorCode) {
            this.error = error;
            this.errorCode = errorCode;
        }

        String getError() {
            return error;
        }

        String getErrorCode() {
            return errorCode;
        }
    }

    private PluginConstants() {
    }
}
