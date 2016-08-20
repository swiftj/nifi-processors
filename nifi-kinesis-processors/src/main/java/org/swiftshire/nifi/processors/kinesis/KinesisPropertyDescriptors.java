/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.swiftshire.nifi.processors.kinesis;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.util.StandardValidators;

/**
 * Common shared Nifi Property Descriptors should go here.
 *
 * @see org.apache.nifi.components.PropertyDescriptor
 */
public abstract class KinesisPropertyDescriptors {
    /**
     * Kinesis stream name
     */
    public static final PropertyDescriptor KINESIS_STREAM_NAME = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Stream Name")
            .name("amazon-kinesis-stream-name")
            .description("The name of Kinesis stream")
            .expressionLanguageSupported(false)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * Batch size of messages to be processed when the processor is invoked
     */
    public static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .displayName("Batch Size")
            .name("batch-size")
            .description("Batch size for messages to be processed on each trigger request (between 1-500).")
            .defaultValue("250")
            .required(false)
            .addValidator(StandardValidators.createLongValidator(1, 500, true))
            .sensitive(false)
            .build();
}