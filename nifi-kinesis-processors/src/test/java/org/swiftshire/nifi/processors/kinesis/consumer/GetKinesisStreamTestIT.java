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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.kinesis.clientlibrary.types.UserRecord;
import com.amazonaws.services.kinesis.model.Record;

import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ExtendedSequenceNumber;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.*;
import static org.swiftshire.nifi.processors.kinesis.consumer.AbstractKinesisConsumerProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.consumer.GetKinesisStream.*;

/**
 * Integration test for the GetKinesisStream processor. Ensure that the Kinesis stream exists before running this test
 */
public class GetKinesisStreamTestIT {

    private TestRunner runner;
    private GetKinesisStream getKinesis;
    private IRecordProcessorCheckpointer mockRecordProcessorCheckPointer;
    private static final String kinesisStream = "ntestkinesis";

    @Before
    public void setUp() throws Exception {
        mockRecordProcessorCheckPointer = Mockito.mock(IRecordProcessorCheckpointer.class);

        getKinesis = new GetKinesisStream();
        runner = TestRunners.newTestRunner(getKinesis);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE, "aws_test_credentials.properties");
        runner.enableControllerService(serviceImpl);
        runner.assertValid(serviceImpl);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
    }

    @After
    public void tearDown() throws Exception {
        getKinesis.onShutdown();
        runner = null;
        getKinesis = null;
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testGetKinesisInvokeOnTriggerIgnored() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 0);
        runner.assertAllFlowFilesTransferred(REL_FAILURE, 0);
    }

    /**
     * Comment out ignore for integration tests (requires creds files)
     */
    @Test
    public void testGetKinesisInvokeProcessRecordsWithOneRecord() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("hello".getBytes());
        runner.run(1);

        List<Record> records = new ArrayList<>();
        Record record = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(ByteBuffer.wrap("hello".getBytes()))
                .withPartitionKey("abcd").withSequenceNumber("seq1");

        UserRecord userRecord = new UserRecord(record);
        records.add(userRecord);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withCheckpointer(mockRecordProcessorCheckPointer)
                .withRecords(records).withMillisBehindLatest(5L);
        ExtendedSequenceNumber esn = new ExtendedSequenceNumber("seq1", 10L);
        InitializationInput initializationInput = new InitializationInput().withShardId("shard1")
                .withExtendedSequenceNumber(esn);

        getKinesis.processRecords(input, initializationInput);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);

        Mockito.verify(mockRecordProcessorCheckPointer, Mockito.times(1)).checkpoint(userRecord);

        final List<MockFlowFile> getFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile flowFile = getFlowFiles.iterator().next();

        Map<String, String> attributes = flowFile.getAttributes();
        assertEquals("abcd", attributes.get(AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY));
        assertEquals("seq1", attributes.get(AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER));
        assertEquals("5", attributes.get(AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND));
        assertEquals("0", attributes.get(AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP));
        assertTrue(attributes.containsKey(KINESIS_CONSUMER_RECORD_START_TIMESTAMP));
        assertEquals("0", attributes.get(KINESIS_CONSUMER_RECORD_NUMBER));

        flowFile.assertContentEquals("hello".getBytes());

        List<ProvenanceEventRecord> events = runner.getProvenanceEvents();

        assertEquals("one item", 1, events.size());
        assertTrue("Should start with kinesis transferuri", events.get(0).getTransitUri().startsWith("kinesis"));
    }

    @Test
    public void testGetKinesisInvokeProcessRecordsWithTwoRecord() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("hello".getBytes());
        runner.run(1);

        List<Record> records = new ArrayList<>();
        Record record1 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(ByteBuffer.wrap("hello1".getBytes()))
                .withPartitionKey("abcd").withSequenceNumber("seq1");
        Record record2 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(ByteBuffer.wrap("hello2".getBytes()))
                .withPartitionKey("abcd").withSequenceNumber("seq2");

        UserRecord userRecord1 = new UserRecord(record1);
        UserRecord userRecord2 = new UserRecord(record2);
        records.add(userRecord1);
        records.add(userRecord2);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withCheckpointer(mockRecordProcessorCheckPointer)
                .withRecords(records).withMillisBehindLatest(5L);

        ExtendedSequenceNumber esn = new ExtendedSequenceNumber("seq1", 10L);

        InitializationInput initializationInput = new InitializationInput().withShardId("shard1")
                .withExtendedSequenceNumber(esn);

        getKinesis.processRecords(input, initializationInput);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);

        Mockito.verify(mockRecordProcessorCheckPointer, Mockito.times(1)).checkpoint(userRecord2);

        final List<MockFlowFile> getFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals("size should be eq", 2, getFlowFiles.size());

        Map<String, String> attributes = getFlowFiles.get(0).getAttributes();
        assertEquals("abcd", attributes.get(AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY));
        assertEquals("seq1", attributes.get(AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER));
        assertEquals("5", attributes.get(AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND));
        assertEquals("0", attributes.get(AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP));
        assertTrue(attributes.containsKey(KINESIS_CONSUMER_RECORD_START_TIMESTAMP));
        assertEquals("0", attributes.get(KINESIS_CONSUMER_RECORD_NUMBER));
        getFlowFiles.get(0).assertContentEquals("hello1".getBytes());

        Map<String, String> attributes2 = getFlowFiles.get(1).getAttributes();
        assertEquals("abcd", attributes2.get(AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY));
        assertEquals("seq2", attributes2.get(AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER));
        assertEquals("5", attributes2.get(AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND));
        assertEquals("0", attributes2.get(AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP));
        assertTrue(attributes2.containsKey(KINESIS_CONSUMER_RECORD_START_TIMESTAMP));
        assertEquals("1", attributes2.get(KINESIS_CONSUMER_RECORD_NUMBER));
        getFlowFiles.get(1).assertContentEquals("hello2".getBytes());

        getKinesis.onShutdown();
    }

    @Test
    public void testGetKinesisInvokeProcessRecordsWithTwoRecordWithSecondRecordDataNull() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("hello".getBytes());
        runner.run(1);

        List<Record> records = new ArrayList<>();
        Record record1 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(ByteBuffer.wrap("hello1".getBytes()))
                .withPartitionKey("abcd").withSequenceNumber("seq1");
        Record record2 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(null)
                .withPartitionKey("abcd").withSequenceNumber("seq2");

        UserRecord userRecord1 = new UserRecord(record1);
        UserRecord userRecord2 = new UserRecord(record2);
        records.add(userRecord1);
        records.add(userRecord2);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withCheckpointer(mockRecordProcessorCheckPointer)
                .withRecords(records).withMillisBehindLatest(5L);

        ExtendedSequenceNumber esn = new ExtendedSequenceNumber("seq1", 10L);
        InitializationInput initializationInput = new InitializationInput().withShardId("shard1")
                .withExtendedSequenceNumber(esn);

        getKinesis.processRecords(input, initializationInput);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS);

        Mockito.verify(mockRecordProcessorCheckPointer, Mockito.times(1)).checkpoint(userRecord1);

        final List<MockFlowFile> getFlowFiles = runner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals("size should be eq", 1, getFlowFiles.size());

        Map<String, String> attributes = getFlowFiles.get(0).getAttributes();
        assertEquals("abcd", attributes.get(AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY));
        assertEquals("seq1", attributes.get(AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER));
        assertEquals("5", attributes.get(AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND));
        assertEquals("0", attributes.get(AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP));
        assertTrue(attributes.containsKey(KINESIS_CONSUMER_RECORD_START_TIMESTAMP));
        assertEquals("0", attributes.get(KINESIS_CONSUMER_RECORD_NUMBER));
        getFlowFiles.get(0).assertContentEquals("hello1".getBytes());

        getKinesis.onShutdown();
    }

    @Test
    public void testGetKinesisInvokeProcessRecordsWithTwoRecordWithFirstRecordDataNull() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("hello".getBytes());
        runner.run(1);

        List<Record> records = new ArrayList<>();
        Record record2 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(ByteBuffer.wrap("hello1".getBytes()))
                .withPartitionKey("abcd").withSequenceNumber("seq1");
        Record record1 = new Record()
                .withApproximateArrivalTimestamp(new Date(0)).withData(null)
                .withPartitionKey("abcd").withSequenceNumber("seq2");

        UserRecord userRecord1 = new UserRecord(record1);
        UserRecord userRecord2 = new UserRecord(record2);
        records.add(userRecord1);
        records.add(userRecord2);
        ProcessRecordsInput input = new ProcessRecordsInput()
                .withCheckpointer(mockRecordProcessorCheckPointer)
                .withRecords(records).withMillisBehindLatest(5L);
        ExtendedSequenceNumber esn = new ExtendedSequenceNumber("seq1", 10L);
        InitializationInput initializationInput = new InitializationInput().withShardId("shard1")
                .withExtendedSequenceNumber(esn);

        getKinesis.processRecords(input, initializationInput);

        Mockito.verify(mockRecordProcessorCheckPointer, Mockito.times(1)).checkpoint(record2);

        final List<MockFlowFile> getFlowFilesSuccess = runner.getFlowFilesForRelationship(REL_SUCCESS);
        assertEquals("success size should be eq", 1, getFlowFilesSuccess.size());
        final List<MockFlowFile> getFlowFilesFailed = runner.getFlowFilesForRelationship(REL_FAILURE);
        assertEquals("failed size should be eq", 0, getFlowFilesFailed.size());
        getKinesis.onShutdown();
    }

    @Test
    public void testGetKinesisShutdown() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_CONSUMER_APPLICATION_NAME, "testapplication");

        runner.assertValid();
        runner.enqueue("hello".getBytes());
        runner.run(1);

        ExtendedSequenceNumber esn = new ExtendedSequenceNumber("seq1", 10L);
        InitializationInput initializationInput = new InitializationInput().withShardId("shard1")
                .withExtendedSequenceNumber(esn);
        ShutdownInput shutdownInput = new ShutdownInput();
        ShutdownReason reason = ShutdownReason.TERMINATE;

        shutdownInput.withCheckpointer(mockRecordProcessorCheckPointer)
                .withShutdownReason(reason);
        getKinesis.shutdown(shutdownInput, initializationInput);

        Mockito.verify(mockRecordProcessorCheckPointer, Mockito.times(1)).checkpoint();

        getKinesis.onShutdown();
    }
}