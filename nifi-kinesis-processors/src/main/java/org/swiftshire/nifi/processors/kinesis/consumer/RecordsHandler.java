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
package org.swiftshire.nifi.processors.kinesis.consumer;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * Interface for handling records used by record processors
 *
 * @see com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor
 * @see KinesisRecordProcessor
 */
public interface RecordsHandler {
    /**
     * Record processor is initialized
     *
     * @param initializationInput initialization input
     */
    void initialize(InitializationInput initializationInput);

    /**
     * Process records
     *
     * @param processRecordsInput process records object
     * @param intializationInput initialization input object
     */
    void processRecords(ProcessRecordsInput processRecordsInput, InitializationInput intializationInput);

    /**
     * Record processor is shutting down
     *
     * @param shutdownInput the shutdown input object
     * @param intializationInput the initialization input object
     */
    void shutdown(ShutdownInput shutdownInput, InitializationInput intializationInput);
}
