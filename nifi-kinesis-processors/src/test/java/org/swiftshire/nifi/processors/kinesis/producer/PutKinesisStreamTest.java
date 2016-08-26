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
package org.swiftshire.nifi.processors.kinesis.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.isA;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.when;

import com.amazonaws.services.kinesis.producer.Attempt;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;
import org.mockito.Matchers;
import org.mockito.Mockito;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.producer.AbstractKinesisProducerProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.producer.PutKinesisStream.*;
import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.*;

@SuppressWarnings("unchecked")
public class PutKinesisStreamTest {

    protected ProcessSession mockProcessSession;
    protected ProcessContext mockProcessContext;

    private TestRunner runner;

    private static final String kinesisStream = "k-stream";

    protected KinesisProducer mockProducer;

    protected PutKinesisStream putKinesis;
    private ListenableFuture<UserRecordResult> mockFuture1;
    private UserRecordResult mockUserRecordResult1;
    private Attempt mockAttempt1;
    private ListenableFuture<UserRecordResult> mockFuture2;
    private UserRecordResult mockUserRecordResult2;
    private Attempt mockAttempt2;
    private List<Attempt> listAttempts;

    private ListenableFuture<UserRecordResult> mockFuture3;
    private UserRecordResult mockUserRecordResult3;

    @Before
    public void setUp() throws Exception {
        putKinesis = new PutKinesisStream() {

            @Override
            protected KinesisProducer makeProducer(KinesisProducerConfiguration config) {
                return mockProducer;
            }

        };

        runner = TestRunners.newTestRunner(putKinesis);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();

        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, ACCESS_KEY, "awsAccessKey");
        runner.setProperty(serviceImpl, SECRET_KEY, "awsSecretKey");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");

        mockProcessSession = Mockito.mock(ProcessSession.class);
        mockProcessContext = Mockito.mock(ProcessContext.class);

        mockProducer = Mockito.mock(KinesisProducer.class);
        mockFuture1 = Mockito.mock(ListenableFuture.class);
        mockFuture2 = Mockito.mock(ListenableFuture.class);
        mockFuture3 = Mockito.mock(ListenableFuture.class);

        listAttempts = new ArrayList<>();

        mockUserRecordResult1 = Mockito.mock(UserRecordResult.class);
        mockUserRecordResult2 = Mockito.mock(UserRecordResult.class);
        mockUserRecordResult3 = Mockito.mock(UserRecordResult.class);

        mockAttempt1 = Mockito.mock(Attempt.class);
        mockAttempt2 = Mockito.mock(Attempt.class);

        Mockito.when(mockAttempt1.getDelay()).thenReturn(1);
        Mockito.when(mockAttempt1.isSuccessful()).thenReturn(true);
        Mockito.when(mockAttempt2.getDelay()).thenReturn(2);
        Mockito.when(mockAttempt2.isSuccessful()).thenReturn(true);
    }

    @Test
    public void testPropertyDescriptors() {
        PutKinesisStream putKinesis = new PutKinesisStream();

        List<PropertyDescriptor> descriptors = putKinesis.getPropertyDescriptors();

        Assert.assertEquals("size should be same", 20, descriptors.size());
        Assert.assertTrue(descriptors.contains(REGION));
        Assert.assertTrue(descriptors.contains(AWS_CREDENTIALS_PROVIDER_SERVICE));
        Assert.assertTrue(descriptors.contains(KINESIS_STREAM_NAME));
        Assert.assertTrue(descriptors.contains(KINESIS_PARTITION_KEY));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_MAX_BUFFER_INTERVAL));
        Assert.assertTrue(descriptors.contains(BATCH_SIZE));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_AGGREGATION_ENABLED));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_AGGREGATION_MAX_COUNT));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_AGGREGATION_MAX_SIZE));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_COLLECTION_MAX_COUNT));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_COLLECTION_MAX_SIZE));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_FAIL_IF_THROTTLED));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_METRICS_NAMESPACE));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_METRICS_GRANULARITY));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_METRICS_LEVEL));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_MAX_PUT_RATE));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_REQUEST_TIMEOUT));
        Assert.assertTrue(descriptors.contains(KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT));
    }

    @Test
    public void testRelationships() {
        Set<Relationship> rels = putKinesis.getRelationships();
        Assert.assertEquals("size should be same", 2, rels.size());
        Assert.assertTrue(rels.contains(REL_FAILURE));
        Assert.assertTrue(rels.contains(REL_SUCCESS));
    }

    @Test
    public void testOnTriggerSuccess1Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1);
        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        listAttempts.add(mockAttempt1);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("test".getBytes(), attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testOnTriggerFailure1Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1);
        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(false);

        listAttempts.add(mockAttempt1);
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("test".getBytes(), attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_FAILURE);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testOnTriggerSuccess2Default() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        Mockito.when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        Mockito.when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult2.isSuccessful()).thenReturn(true);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("success1".getBytes(), attrib);
        runner.enqueue("success2".getBytes(), attrib);
        runner.run(1);
        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count == 1) {
                flowFile.assertContentEquals("success1".getBytes());
            }

            if (count == 2) {
                flowFile.assertContentEquals("success2".getBytes());
            }

            count++;
        }
    }

    @Test
    public void testOnTrigger2FailedBothDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(false);

        Mockito.when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        Mockito.when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("failure1".getBytes(), attrib);
        runner.enqueue("failure2".getBytes(), attrib);
        runner.run(1);

        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_FAILURE);
        int count = 1;
        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count == 1) {
                flowFile.assertContentEquals("failure1".getBytes());
            }

            if (count == 2) {
                flowFile.assertContentEquals("failure2".getBytes());
            }

            count++;
        }
    }

    @Test
    public void testOnTrigger2FirstFailedSecondSuccessDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(false);

        Mockito.when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        Mockito.when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult2.isSuccessful()).thenReturn(true);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("failure1".getBytes(), attrib);
        runner.enqueue("success2".getBytes(), attrib);
        runner.run(1);

        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_FAILURE);
        int count = 1;

        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count == 1) {
                flowFile.assertContentEquals("failure1".getBytes());
            }
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(REL_SUCCESS);
        int count2 = 1;
        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count2 == 1) {
                flowFile.assertContentEquals("success2".getBytes());
            }
        }
    }

    @Test
    public void testOnTrigger2FirstSucessSecondFailedDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        Mockito.when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        Mockito.when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("success1".getBytes(), attrib);
        runner.enqueue("failure2".getBytes(), attrib);
        runner.run(1);

        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_FAILURE);
        int count = 1;

        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count == 1) {
                flowFile.assertContentEquals("failure2".getBytes());
            }
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(REL_SUCCESS);
        int count2 = 1;

        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count2 == 1) {
                flowFile.assertContentEquals("success1".getBytes());
            }
        }
    }

    @Test
    public void testOnTrigger3FirstSucessSecondFailedThirdSuccessDefault() throws Exception {
        putKinesis.setProducer(mockProducer);

        Mockito.when(mockProducer.addUserRecord(
                Matchers.anyString(), Matchers.anyString(), Matchers.isA(ByteBuffer.class))).thenReturn(mockFuture1, mockFuture2);

        Mockito.when(mockFuture1.get()).thenReturn(mockUserRecordResult1);
        Mockito.when(mockUserRecordResult1.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult1.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult1.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult1.isSuccessful()).thenReturn(true);

        Mockito.when(mockFuture2.get()).thenReturn(mockUserRecordResult2);
        Mockito.when(mockUserRecordResult2.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult2.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult2.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult2.isSuccessful()).thenReturn(false);

        Mockito.when(mockFuture3.get()).thenReturn(mockUserRecordResult3);
        Mockito.when(mockUserRecordResult3.getAttempts()).thenReturn(listAttempts);
        Mockito.when(mockUserRecordResult3.getSequenceNumber()).thenReturn("seq1");
        Mockito.when(mockUserRecordResult3.getShardId()).thenReturn("shard1");
        Mockito.when(mockUserRecordResult3.isSuccessful()).thenReturn(true);

        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);

        Map<String, String> attrib = new HashMap<String, String>() {{
            put("kinesis.partition.key", "p1");
        }};

        runner.enqueue("success1".getBytes(), attrib);
        runner.enqueue("failure2".getBytes(), attrib);
        runner.enqueue("success3".getBytes(), attrib);
        runner.run(1);

        listAttempts.add(mockAttempt1);
        listAttempts.add(mockAttempt2);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_FAILURE);
        int count = 1;

        for (MockFlowFile flowFile : ffs) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count == 1) {
                flowFile.assertContentEquals("failure2".getBytes());
            }

            count++;
        }

        final List<MockFlowFile> ffs2 = runner.getFlowFilesForRelationship(REL_SUCCESS);
        int count2 = 1;

        for (MockFlowFile flowFile : ffs2) {

            Map<String, String> attributes = flowFile.getAttributes();
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
            Assert.assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

            if (count2 == 1) {
                flowFile.assertContentEquals("success1".getBytes());
            }

            if (count2 == 2) {
                flowFile.assertContentEquals("success3".getBytes());
            }

            count++;
        }
    }
}