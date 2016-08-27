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

import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderControllerService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.KINESIS_STREAM_NAME;
import static org.swiftshire.nifi.processors.kinesis.producer.PutKinesisStream.*;

/**
 * Ensure that the Kinesis stream exists before running the test
 */
public class PutKinesisStreamTestIT {

    private TestRunner runner;

    private static final String kinesisStream = "ntestkinesis";

    @Before
    public void setUp() throws Exception {
        runner = TestRunners.newTestRunner(PutKinesisStream.class);
        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE, "mock-aws-credentials.properties");
        runner.enableControllerService(serviceImpl);

        runner.assertValid(serviceImpl);
        runner.setProperty(AWS_CREDENTIALS_PROVIDER_SERVICE, "awsCredentialsProvider");
    }

    @After
    public void tearDown() throws Exception {
        runner = null;
    }

    @Test
    public void testIntegrationSuccessWithPartitionKey() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.assertValid();
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testIntegrationSuccessWithPartitionKeyDefaultSetting() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.assertValid();
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        attrib.put("kinesis.partition.key", "p1");
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testIntegrationSuccessWithOutPartitionKey() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, kinesisStream);
        runner.assertValid();
        runner.setProperty(KINESIS_PARTITION_KEY, "${kinesis.partition.key}");
        runner.setValidateExpressionUsage(true);
        Map<String,String> attrib = new HashMap<>();
        runner.enqueue("test".getBytes(),attrib);
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_SUCCESS, 1);

        final List<MockFlowFile> ffs = runner.getFlowFilesForRelationship(REL_SUCCESS);
        final MockFlowFile out = ffs.iterator().next();

        Map<String, String> attributes = out.getAttributes();
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID));
        assertTrue(attributes.containsKey(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER));

        out.assertContentEquals("test".getBytes());
    }

    @Test
    public void testIntegrationFailedBadStreamName() throws Exception {
        runner.setProperty(KINESIS_STREAM_NAME, "bad-stream");
        runner.assertValid();

        runner.enqueue("test".getBytes());
        runner.run(1);

        runner.assertAllFlowFilesTransferred(REL_FAILURE, 1);

    }
}