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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Set;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;

public class AbstractKinesisConsumerProcessorTest {

    protected AbstractKinesisConsumerProcessor processor;
    protected ProcessSession mockProcessSession1;
    protected ProcessSession mockProcessSession2;
    protected ProcessSessionFactory mockProcessSessionFactory1;
    protected ProcessSessionFactory mockProcessSessionFactory2;
    protected ProcessContext mockProcessContext;

    @Before
    public void setUp() {
        mockProcessSession1 = Mockito.mock(ProcessSession.class);
        mockProcessSession2 = Mockito.mock(ProcessSession.class);
        mockProcessSessionFactory1 = Mockito.mock(ProcessSessionFactory.class);
        mockProcessSessionFactory2 = Mockito.mock(ProcessSessionFactory.class);
        mockProcessContext = Mockito.mock(ProcessContext.class);

        processor = new AbstractKinesisConsumerProcessor() {
            public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            }
        };
    }

    @Test
    public void testOnTriggerProcessContextProcessSessionFactoryCalledOnce() {
        Mockito.when(mockProcessSessionFactory1.createSession()).thenReturn(mockProcessSession1);
        processor.onTrigger(mockProcessContext, mockProcessSessionFactory1);
        ProcessSessionFactory sessionFactory = processor.getSessionFactory();
        assertSame(sessionFactory, mockProcessSessionFactory1);
        Mockito.verify(mockProcessSessionFactory1, Mockito.times(1)).createSession();
        Mockito.verify(mockProcessSession1).commit();
    }

    @Test
    public void testOnTriggerProcessContextProcessSessionFactoryCalledMoreThanOnce() {
        Mockito.when(mockProcessSessionFactory1.createSession()).thenReturn(mockProcessSession1, mockProcessSession2);

        processor.onTrigger(mockProcessContext, mockProcessSessionFactory1);

        ProcessSessionFactory sessionFactory = processor.getSessionFactory();
        assertSame(sessionFactory, mockProcessSessionFactory1);

        processor.onTrigger(mockProcessContext, mockProcessSessionFactory2);

        sessionFactory = processor.getSessionFactory();
        assertSame(sessionFactory, mockProcessSessionFactory1);

        Mockito.verify(mockProcessSessionFactory1, Mockito.times(2)).createSession();
        Mockito.verify(mockProcessSession1).commit();
        Mockito.verify(mockProcessSession2).commit();
    }

    @Test
    public void testInitialPositionsAllowableValues() {
        Set<String> initialPositions = AbstractKinesisConsumerProcessor.getInitialPositions();
        assertEquals(2, initialPositions.size());
        assertTrue(initialPositions.contains(InitialPositionInStream.LATEST.toString()));
        assertTrue(initialPositions.contains(InitialPositionInStream.TRIM_HORIZON.toString()));
    }

    @Test
    public void testMetricsLevelAllowableValues() {
        Set<String> values = AbstractKinesisConsumerProcessor.getMetricsAllowableValues();
        assertEquals(3, values.size());
        assertTrue(values.contains(MetricsLevel.DETAILED.toString()));
        assertTrue(values.contains(MetricsLevel.SUMMARY.toString()));
        assertTrue(values.contains(MetricsLevel.NONE.toString()));
    }
}