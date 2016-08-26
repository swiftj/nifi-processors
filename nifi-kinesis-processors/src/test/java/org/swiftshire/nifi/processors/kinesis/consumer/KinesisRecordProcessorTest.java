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

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;

public class KinesisRecordProcessorTest {

    protected RecordsHandler mockDelegate;
    protected InitializationInput mockInitializationInput;
    protected ProcessRecordsInput mockProcessRecordsInput;
    protected ShutdownInput mockShutdownInput;

    protected KinesisRecordProcessor processor;

    @Before
    public void setUp() throws Exception {
        mockDelegate = Mockito.mock(RecordsHandler.class);
        mockInitializationInput = Mockito.mock(InitializationInput.class);
        mockProcessRecordsInput = Mockito.mock(ProcessRecordsInput.class);
        mockShutdownInput = Mockito.mock(ShutdownInput.class);
        processor = new KinesisRecordProcessor(mockDelegate);
    }

    @Test
    public void testConstructor() {
        assertSame(mockDelegate, processor.getDelegate());
    }

    @Test
    public void testInitialize() {
        processor.initialize(mockInitializationInput);
        assertSame(mockDelegate, processor.getDelegate());
        Mockito.verify(mockDelegate).initialize(mockInitializationInput);
    }

    @Test
    public void testInitializeAndProcessRecords() {
        processor.initialize(mockInitializationInput);
        processor.processRecords(mockProcessRecordsInput);
        assertSame(mockDelegate, processor.getDelegate());
        Mockito.verify(mockDelegate).initialize(mockInitializationInput);
        Mockito.verify(mockDelegate).processRecords(mockProcessRecordsInput,
                mockInitializationInput);
    }

    @Test
    public void testInitializeAndShutdown() {
        processor.initialize(mockInitializationInput);
        processor.shutdown(mockShutdownInput);
        assertSame(mockDelegate, processor.getDelegate());
        Mockito.verify(mockDelegate).initialize(mockInitializationInput);
        Mockito.verify(mockDelegate).shutdown(mockShutdownInput, mockInitializationInput);
    }
}