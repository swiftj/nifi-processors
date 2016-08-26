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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.provenance.ProvenanceReporter;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import com.amazonaws.services.kinesis.model.Record;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.*;
import static org.swiftshire.nifi.processors.kinesis.consumer.AbstractKinesisConsumerProcessor.*;

@SuppressWarnings("unchecked")
public class GetKinesisStreamTest {

    protected ProcessSession mockProcessSession;
    protected ProcessContext mockProcessContext;
    protected InitializationInput mockInitializationInput;
    protected ProcessSessionFactory mockProcessSessionFactory;
    protected ProcessRecordsInput processRecordsInput;
    protected List<Record> records;
    protected IRecordProcessorCheckpointer mockRecordProcessorCheckpointer;
    protected ProvenanceReporter mockProvenanceReporter;
    protected FlowFile mockFlowFile1;
    protected FlowFile mockFlowFile2;

    protected AWSCredentialsProviderControllerService mockAWSCredentialsProviderControllerService;
    protected PropertyValue mockPropertyValue;

    protected GetKinesisStream getKinesis;
    private TestRunner runner;
    protected Worker mockWorker;
    private ShutdownInput mockShutdownInput;
    protected boolean makeWorkerThrowsException = false;

    @Before
    public void setUp() throws Exception {
        getKinesis = new GetKinesisStream() {
            @Override
            protected ProcessSessionFactory getSessionFactory() {
                return mockProcessSessionFactory;
            }

            @Override
            protected Worker makeWorker(KinesisClientLibConfiguration config,
                                        KinesisRecordProcessorFactory kinesisRecordProcessorFactory) {
                if (makeWorkerThrowsException) {
                    throw new RuntimeException("Worker");
                }
                else {
                    return mockWorker;
                }
            }

            @Override
            protected AWSCredentialsProviderService getAWSCredentialsProviderService(ProcessContext context) {
                return mockAWSCredentialsProviderControllerService;
            }

        };
        runner = TestRunners.newTestRunner(getKinesis);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY,
                "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY,
                "awsSecretKey");
        runner.enableControllerService(serviceImpl);

        runner.setProperty(REGION, "us-west-2");
        runner.setProperty(KINESIS_STREAM_NAME, "streamname");
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "AppName");
        runner.setProperty(KINESIS_CONSUMER_WORKER_ID_PREFIX, "WorkerPrefix1");
        runner.setProperty(BATCH_SIZE, "5");
        runner.setProperty(KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM,
                InitialPositionInStream.LATEST.toString());
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS, "2000");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_MAX_RECORDS, "25");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS, "200");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST, "true");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS, "100");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS, "200");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION, "true");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS, "200");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS, "200");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE, "20");
        runner.setProperty(KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL, MetricsLevel.DETAILED.name());
        runner.assertValid();

        mockProcessSession = mock(ProcessSession.class);
        mockProcessSessionFactory = mock(ProcessSessionFactory.class);
        mockProcessContext = mock(ProcessContext.class);

        mockInitializationInput = mock(InitializationInput.class);
        mockShutdownInput = mock(ShutdownInput.class);

        mockRecordProcessorCheckpointer = mock(IRecordProcessorCheckpointer.class);
        mockProvenanceReporter = mock(ProvenanceReporter.class);
        mockPropertyValue = mock(PropertyValue.class);
        mockFlowFile1 = mock(FlowFile.class);
        mockFlowFile2 = mock(FlowFile.class);

        records = new ArrayList<>();
        processRecordsInput = new ProcessRecordsInput().withCheckpointer(mockRecordProcessorCheckpointer)
                .withRecords(records).withMillisBehindLatest(500L);

        mockAWSCredentialsProviderControllerService = mock(AWSCredentialsProviderControllerService.class);
        mockWorker = mock(Worker.class);
    }

    @Test
    public void testPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = getKinesis.getPropertyDescriptors();
        assertEquals("size should be same", 18, descriptors.size());
        assertTrue(descriptors.contains(REGION));
        assertTrue(descriptors.contains(AWS_CREDENTIALS_PROVIDER_SERVICE));
        assertTrue(descriptors.contains(KINESIS_STREAM_NAME));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_APPLICATION_NAME));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_WORKER_ID_PREFIX));
        assertTrue(descriptors.contains(BATCH_SIZE));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_MAX_RECORDS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE));
        assertTrue(descriptors.contains(KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL));
    }

    @Test
    public void testRelationships() {
        Set<Relationship> rels = getKinesis.getRelationships();
        assertEquals("size should be same", 1, rels.size());
        assertTrue(rels.contains(REL_SUCCESS));
    }

    @Test
    public void testOnTriggerCallsYield() throws Exception {
        getKinesis.onTrigger(mockProcessContext, null);
        verify(mockProcessContext, times(1)).yield();
    }

    @Test
    public void testProcessOneRecordSuccess() throws Exception {
        Record record = new Record();
        Date arrivalTimestamp = new Date();
        record.setData(ByteBuffer.wrap("hello".getBytes("UTF-8")));
        record.setApproximateArrivalTimestamp(arrivalTimestamp);
        records.add(record);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).transfer(mockFlowFile1, REL_SUCCESS);
        verify(mockProvenanceReporter).receive(eq(mockFlowFile1), anyString(), isA(Long.class));
        verify(mockRecordProcessorCheckpointer).checkpoint(record);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testOnScheduledSuccess() throws Exception {
        when(mockShutdownInput.getCheckpointer()).thenReturn(mockRecordProcessorCheckpointer);
        ProcessContext context = runner.getProcessContext();
        getKinesis.onScheduled(context);
        getKinesis.shutdown(mockShutdownInput, mockInitializationInput);
        verify(mockShutdownInput).getCheckpointer();
        verify(mockShutdownInput).getShutdownReason();
        verify(mockInitializationInput).getShardId();
        verify(mockInitializationInput).getExtendedSequenceNumber();
        verify(mockRecordProcessorCheckpointer).checkpoint();
    }

    @Test(expected = ProcessException.class)
    public void testOnScheduledWorkerThrowsException() throws Exception {
        when(mockShutdownInput.getCheckpointer()).thenReturn(mockRecordProcessorCheckpointer);
        ProcessContext context = runner.getProcessContext();
        makeWorkerThrowsException = true;
        getKinesis.onScheduled(context);
    }

    @Test
    public void testProcessOneRecordSuccessCheckPointThrowsExceception() throws Exception {
        Record record = new Record();
        Date arrivalTimestamp = new Date();
        record.setData(ByteBuffer.wrap("hello".getBytes("UTF-8")));
        record.setApproximateArrivalTimestamp(arrivalTimestamp);
        records.add(record);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);
        doThrow(new ShutdownException("shutdown")).when(mockRecordProcessorCheckpointer).checkpoint(record);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).transfer(mockFlowFile1, REL_SUCCESS);
        verify(mockProvenanceReporter).receive(eq(mockFlowFile1), anyString(), isA(Long.class));
        verify(mockRecordProcessorCheckpointer).checkpoint(record);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testProcessTwoRecordSuccess() throws Exception {
        Record record1 = new Record();
        Date arrivalTimestamp1 = new Date();
        record1.setData(ByteBuffer.wrap("hello1".getBytes("UTF-8")));
        record1.setApproximateArrivalTimestamp(arrivalTimestamp1);
        records.add(record1);

        Record record2 = new Record();
        Date arrivalTimestamp2 = new Date();
        record2.setData(ByteBuffer.wrap("hello2".getBytes("UTF-8")));
        record2.setApproximateArrivalTimestamp(arrivalTimestamp2);
        records.add(record2);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1, mockFlowFile2);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);

        when(mockProcessSession.putAllAttributes(eq(mockFlowFile2), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile2);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2)))
                .thenReturn(mockFlowFile2);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).transfer(mockFlowFile1, REL_SUCCESS);
        verify(mockProcessSession).putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class));

        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2));
        verify(mockProcessSession).transfer(mockFlowFile2, REL_SUCCESS);
        verify(mockProcessSession).putAllAttributes(eq(mockFlowFile2), (Map<String, String>) isA(Map.class));

        verify(mockProvenanceReporter, times(1)).receive(eq(mockFlowFile1), anyString(), isA(Long.class));
        verify(mockProvenanceReporter, times(1)).receive(eq(mockFlowFile2), anyString(), isA(Long.class));

        verify(mockRecordProcessorCheckpointer, times(1)).checkpoint(record2);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testProcessTwoRecordFailOnFirstImport() throws Exception {
        Record record1 = new Record();
        Date arrivalTimestamp1 = new Date();
        record1.setData(ByteBuffer.wrap("hello1".getBytes("UTF-8")));
        record1.setApproximateArrivalTimestamp(arrivalTimestamp1);
        records.add(record1);

        Record record2 = new Record();
        Date arrivalTimestamp2 = new Date();
        record2.setData(ByteBuffer.wrap("hello2".getBytes("UTF-8")));
        record2.setApproximateArrivalTimestamp(arrivalTimestamp2);
        records.add(record2);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1, mockFlowFile2);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenThrow(new RuntimeException("testexception"));
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2)))
                .thenReturn(mockFlowFile2);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2));
        verify(mockRecordProcessorCheckpointer, times(1)).checkpoint(record2);
        verify(mockProcessSession).remove(mockFlowFile1);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testProcessTwoRecordFailOnFirstTransfer() throws Exception {
        Record record1 = new Record();
        Date arrivalTimestamp1 = new Date();
        record1.setData(ByteBuffer.wrap("hello1".getBytes("UTF-8")));
        record1.setApproximateArrivalTimestamp(arrivalTimestamp1);
        records.add(record1);

        Record record2 = new Record();
        Date arrivalTimestamp2 = new Date();
        record2.setData(ByteBuffer.wrap("hello2".getBytes("UTF-8")));
        record2.setApproximateArrivalTimestamp(arrivalTimestamp2);
        records.add(record2);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1, mockFlowFile2);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);

        doThrow(new RuntimeException("testException")).when(mockProcessSession)
                .transfer(mockFlowFile1, REL_SUCCESS);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class));

        verify(mockProcessSession).remove(mockFlowFile1);
        verify(mockRecordProcessorCheckpointer, times(1)).checkpoint(record2);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testProcessTwoRecordFailOnSecondImport() throws Exception {
        Record record1 = new Record();
        Date arrivalTimestamp1 = new Date();
        record1.setData(ByteBuffer.wrap("hello1".getBytes("UTF-8")));
        record1.setApproximateArrivalTimestamp(arrivalTimestamp1);
        records.add(record1);

        Record record2 = new Record();
        Date arrivalTimestamp2 = new Date();
        record2.setData(ByteBuffer.wrap("hello2".getBytes("UTF-8")));
        record2.setApproximateArrivalTimestamp(arrivalTimestamp2);
        records.add(record2);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1, mockFlowFile2);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile2), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile2);

        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);

        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2)))
                .thenThrow(new RuntimeException("RuntimeExceptionImport"));

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).transfer(mockFlowFile1, REL_SUCCESS);
        verify(mockProcessSession).putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class));

        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2));

        verify(mockProvenanceReporter, times(1)).receive(eq(mockFlowFile1), anyString(), isA(Long.class));
        verify(mockProcessSession).remove(mockFlowFile2);
        verify(mockRecordProcessorCheckpointer, times(1)).checkpoint(record1);
        verify(mockProcessSession).commit();
    }

    @Test
    public void testProcessTwoRecordFailOnSecondTransfer() throws Exception {
        Record record1 = new Record();
        Date arrivalTimestamp1 = new Date();
        record1.setData(ByteBuffer.wrap("hello1".getBytes("UTF-8")));
        record1.setApproximateArrivalTimestamp(arrivalTimestamp1);
        records.add(record1);

        Record record2 = new Record();
        Date arrivalTimestamp2 = new Date();
        record2.setData(ByteBuffer.wrap("hello2".getBytes("UTF-8")));
        record2.setApproximateArrivalTimestamp(arrivalTimestamp2);
        records.add(record2);

        when(mockProcessSessionFactory.createSession()).thenReturn(mockProcessSession);
        when(mockProcessSession.create()).thenReturn(mockFlowFile1, mockFlowFile2);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.putAllAttributes(eq(mockFlowFile2), (Map<String, String>) isA(Map.class)))
                .thenReturn(mockFlowFile2);

        when(mockProcessSession.getProvenanceReporter()).thenReturn(mockProvenanceReporter);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1)))
                .thenReturn(mockFlowFile1);
        when(mockProcessSession.importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2)))
                .thenReturn(mockFlowFile2);

        doThrow(new RuntimeException("RuntimeExceptionTransfer")).when(mockProcessSession)
                .transfer(mockFlowFile2, REL_SUCCESS);

        getKinesis.processRecords(processRecordsInput, mockInitializationInput);

        verify(mockProcessSessionFactory).createSession();
        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile1));
        verify(mockProcessSession).transfer(mockFlowFile1, REL_SUCCESS);
        verify(mockProcessSession).putAllAttributes(eq(mockFlowFile1), (Map<String, String>) isA(Map.class));

        verify(mockProcessSession).importFrom(isA(ByteArrayInputStream.class), eq(mockFlowFile2));
        verify(mockProcessSession).transfer(mockFlowFile2, REL_SUCCESS);

        verify(mockProvenanceReporter, times(1)).receive(eq(mockFlowFile1), anyString(), isA(Long.class));
        verify(mockProcessSession).remove(mockFlowFile2);
        verify(mockRecordProcessorCheckpointer, times(1)).checkpoint(record1);
        verify(mockProcessSession).commit();
    }
}