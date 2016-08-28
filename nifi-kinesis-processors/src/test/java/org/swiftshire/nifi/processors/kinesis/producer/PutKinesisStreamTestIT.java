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

import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
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
 * Integration test for the {@link PutKinesisStream} Apache Nifi Processor that uses the standalone
 * <a href="https://github.com/mhart/kinesalite">Kinesalite</a> testing framework.
 *
 * <p>Ensure that the Kinesalite system is up and running first and the matching stream exists <b>before</b>
 * running this test. For example, run Kinesalite locally from the command line...<p/>
 * <code>
 *  $ kinesalite --ssl
 * </code>
 * <p>Next, use the AWS command line tool to connect to the Kinesalite server and create the Kinesis stream
 * this integration test uses for testing purposes...
 * <code>
 *  $ aws --no-verify-ssl kinesis create-stream --stream-name kinesalite --shard-count 1 --endpoint-url 'https://localhost:4567'
 *  $ aws --no-verify-ssl kinesis list-streams --endpoint-url 'https://localhost:4567'
 *  {
 *      "StreamNames": [
 *          "kinesalite"
 *      ]
 *  }
 * </code>
 * <p>This is all that's needed to execute this integration test.
 *
 * @since 1.0
 */
public class PutKinesisStreamTestIT {
    /**
     * Path to the project provided AWS credentials to test with.
     */
    private static final String AWS_CREDS_PROPERTIES_FILE =
            "nifi-kinesis-processors/src/test/resources/mock-aws-credentials.properties";

    /**
     * Name of the AWS Kinesis Stream that we're going to test with
     */
    private static final String TEST_STREAM_NAME = "kinesalite";

    /**
     * Apache Nifi test runner we use to execute the test.
     */
    private TestRunner runner;

    /**
     * Setup this integration test so we can connect to the locally running Kinesalite service.
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
        // Object under test
        final PutKinesisStream processor = new PutKinesisStream() {
            @Override
            protected KinesisProducer makeProducer(KinesisProducerConfiguration config) {
                // We use Kinesalite for our IT testing so we need to explicitly
                // set these configuration items so our test works with it running
                // locally. We use all Kinesalite's default settings except for SSL
                // which must be turned on in Kinesalite because the KPL forces it.
                config.setCustomEndpoint("127.0.0.1");
                config.setPort(4567);
                config.setVerifyCertificate(false); // Allow self-signed certs
                config.setRegion("us-east-1");

                return new KinesisProducer(config);
            }
        };

        runner = TestRunners.newTestRunner(processor);

        final AWSCredentialsProviderControllerService serviceImpl = new AWSCredentialsProviderControllerService();
        runner.addControllerService("awsCredentialsProvider", serviceImpl);
        runner.setProperty(serviceImpl, CREDENTIALS_FILE, AWS_CREDS_PROPERTIES_FILE);
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
        runner.setProperty(KINESIS_STREAM_NAME, TEST_STREAM_NAME);
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
        runner.setProperty(KINESIS_STREAM_NAME, TEST_STREAM_NAME);
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
        runner.setProperty(KINESIS_STREAM_NAME, TEST_STREAM_NAME);
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