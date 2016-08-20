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

import com.amazonaws.services.kinesis.clientlibrary.interfaces.v2.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

/**
 * Implementation of record processor which delegates the call to the
 * records handler
 *
 * @see RecordsHandler
 */
public class KinesisRecordProcessor implements IRecordProcessor {
    /**
     * The record handler delegate
     */
    protected RecordsHandler delegate;

    /**
     * The initialization input
     */
    protected InitializationInput initializationInput;

    /**
     * The kinesis record processor constructor
     *
     * @param recordsHandler the record handler
     */
    public KinesisRecordProcessor(RecordsHandler recordsHandler) {
        this.delegate = recordsHandler;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput initializationInput) {
        this.initializationInput = initializationInput;
        delegate.initialize(initializationInput);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        delegate.processRecords(processRecordsInput, initializationInput);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput) {
        delegate.shutdown(shutdownInput, initializationInput);
    }

    /**
     * The delegate record handler
     *
     * @return the records handler
     */
    protected RecordsHandler getDelegate() {
        return delegate;
    }
}
