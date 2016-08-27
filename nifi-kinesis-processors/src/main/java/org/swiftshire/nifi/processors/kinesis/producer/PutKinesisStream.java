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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.producer.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.*;

@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"amazon", "aws", "kinesis", "put", "stream"})
@CapabilityDescription("Sends the contents of the FlowFile to a specified Amazon Kinesis stream."
        + " This Kinesis processor uses the AWS Kinesis Client Library which automatically uses DynamoDB to store client state and CloudWatch to store metrics."
        + " AWS credentials used by this processor must have permissions to access to those AWS services."
        + " Consequently, use of this processor may incur unexpected AWS account costs."
)
@ReadsAttribute(attribute = PutKinesisStream.AWS_KINESIS_PARTITION_KEY, description = "Partition key to be used for publishing data to Kinesis.  If it is not available then a random key used")
@WritesAttributes({
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL, description = "Was the record posted successfully"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SHARD_ID, description = "Shard id where the record was posted"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER, description = "Sequence number of the posted record"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT, description = "Number of attempts for posting the record"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX, description =
                "Attempt error code for each attempt"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX, description =
                "Attempt delay for each attempt"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX, description =
                "Attempt error message for each attempt"),
        @WritesAttribute(attribute = PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + "<n>" + PutKinesisStream.AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX, description =
                "Attempt successful for each attempt")
})
public class PutKinesisStream extends AbstractKinesisProducerProcessor {
    /**
     * Attributes written by the producer
     */
    public static final String AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL = "aws.kinesis.producer.record.successful";
    public static final String AWS_KINESIS_PRODUCER_RECORD_SHARD_ID = "aws.kinesis.producer.record.shard.id";
    public static final String AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER = "aws.kinesis.producer.record.sequencenumber";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX = "aws.kinesis.producer.record.attempt.";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX = ".error.code";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX = ".delay";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX = ".error.message";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX = ".successful";
    public static final String AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT = "aws.kinesis.producer.record.attempts.count";

    /**
     * Attributes read the producer
     */
    public static final String AWS_KINESIS_PARTITION_KEY = "kinesis.partition.key";

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(
                    REGION,
                    AWS_CREDENTIALS_PROVIDER_SERVICE,
                    KINESIS_STREAM_NAME,
                    KINESIS_PARTITION_KEY,
                    KINESIS_PRODUCER_MAX_BUFFER_INTERVAL,
                    KINESIS_PRODUCER_AGGREGATION_ENABLED,
                    KINESIS_PRODUCER_AGGREGATION_MAX_COUNT,
                    KINESIS_PRODUCER_AGGREGATION_MAX_SIZE,
                    KINESIS_PRODUCER_COLLECTION_MAX_COUNT,
                    KINESIS_PRODUCER_COLLECTION_MAX_SIZE,
                    KINESIS_PRODUCER_FAIL_IF_THROTTLED,
                    KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND,
                    KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND,
                    KINESIS_PRODUCER_METRICS_NAMESPACE,
                    KINESIS_PRODUCER_METRICS_GRANULARITY,
                    KINESIS_PRODUCER_METRICS_LEVEL,
                    KINESIS_PRODUCER_MAX_PUT_RATE,
                    KINESIS_PRODUCER_REQUEST_TIMEOUT,
                    KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT,
                    BATCH_SIZE
            ));

    /**
     * AWS KCL producer object we use internally
     */
    private KinesisProducer producer;

    /**
     * PRNG we use to create random Kinesis shard keys to distribute the load
     */
    private Random randomGenerator;

    /**
     * @return the producer
     */
    protected KinesisProducer getProducer() {
        return producer;
    }

    /**
     * @param producer the producer to set
     */
    protected void setProducer(KinesisProducer producer) {
        this.producer = producer;
    }

    /**
     * @return the randomGenerator
     */
    protected Random getRandomGenerator() {
        return randomGenerator;
    }

    /**
     * @param randomGenerator the randomGenerator to set
     */
    protected void setRandomGenerator(Random randomGenerator) {
        this.randomGenerator = randomGenerator;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    /**
     * {@inheritDoc}
     */
    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        randomGenerator = new Random();

        KinesisProducerConfiguration config = new KinesisProducerConfiguration();

        config.setRegion(context.getProperty(REGION).getValue());

        final AWSCredentialsProviderService awsCredentialsProviderService =
                context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        config.setCredentialsProvider(awsCredentialsProviderService.getCredentialsProvider());

        config.setMaxConnections(context.getProperty(KINESIS_PRODUCER_MAX_CONNECTIONS_TO_BACKEND).asInteger());
        config.setMinConnections(context.getProperty(KINESIS_PRODUCER_MIN_CONNECTIONS_TO_BACKEND).asInteger());

        config.setRequestTimeout(context.getProperty(KINESIS_PRODUCER_REQUEST_TIMEOUT).asInteger());
        config.setConnectTimeout(context.getProperty(KINESIS_PRODUCER_TLS_CONNECT_TIMEOUT).asLong());

        config.setRecordMaxBufferedTime(context.getProperty(KINESIS_PRODUCER_MAX_BUFFER_INTERVAL).asInteger());
        config.setRateLimit(context.getProperty(KINESIS_PRODUCER_MAX_PUT_RATE).asInteger());

        config.setAggregationEnabled(context.getProperty(KINESIS_PRODUCER_AGGREGATION_ENABLED).asBoolean());
        config.setAggregationMaxCount(context.getProperty(KINESIS_PRODUCER_AGGREGATION_MAX_COUNT).asLong());
        config.setAggregationMaxSize(context.getProperty(KINESIS_PRODUCER_AGGREGATION_MAX_SIZE).asInteger());

        config.setCollectionMaxCount(context.getProperty(KINESIS_PRODUCER_COLLECTION_MAX_COUNT).asInteger());
        config.setCollectionMaxSize(context.getProperty(KINESIS_PRODUCER_COLLECTION_MAX_SIZE).asInteger());

        config.setFailIfThrottled(context.getProperty(KINESIS_PRODUCER_FAIL_IF_THROTTLED).asBoolean());

        config.setMetricsCredentialsProvider(awsCredentialsProviderService.getCredentialsProvider());
        config.setMetricsNamespace(context.getProperty(KINESIS_PRODUCER_METRICS_NAMESPACE).getValue());
        config.setMetricsGranularity(context.getProperty(KINESIS_PRODUCER_METRICS_GRANULARITY).getValue());
        config.setMetricsLevel(context.getProperty(KINESIS_PRODUCER_METRICS_LEVEL).getValue());

        producer = makeProducer(config);

    }

    /**
     * Make producer helper method
     *
     * @param config Kinesis producer configuration
     * @return the Kinesis producer
     */
    protected KinesisProducer makeProducer(KinesisProducerConfiguration config) {
        return new KinesisProducer(config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmazonKinesisClient createClient(ProcessContext context, AWSCredentials credentials, ClientConfiguration config) {
        final ComponentLog log = getLogger();

        if (log.isInfoEnabled()) {
            log.info("Creating Amazon Kinesis Client using AWS credentials provider (deprecated)");
        }

        return new AmazonKinesisClient(credentials, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected AmazonKinesisClient createClient(ProcessContext context, AWSCredentialsProvider credentialsProvider, ClientConfiguration config) {
        final ComponentLog log = getLogger();

        if (log.isInfoEnabled()) {
            log.info("Creating Amazon Kinesis Client using AWS credentials provider");
        }

        return new AmazonKinesisClient(credentialsProvider, config);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        final int batchSize = context.getProperty(BATCH_SIZE).asInteger();

        List<FlowFile> flowFiles = session.get(batchSize);

        if (flowFiles == null || flowFiles.size() == 0) {
            return;
        }

        final String stream = context.getProperty(KINESIS_STREAM_NAME).getValue();
        final ComponentLog log = getLogger();

        try {
            List<Future<UserRecordResult>> addRecordFutures = new ArrayList<>();

            // Prepare batch of records
            for (FlowFile flowFile1 : flowFiles) {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();

                session.exportTo(flowFile1, baos);

                String partitionKey = context.getProperty(KINESIS_PARTITION_KEY)
                        .evaluateAttributeExpressions(flowFile1).getValue();

                if (StringUtils.isBlank(partitionKey)) {
                    partitionKey = Integer.toString(randomGenerator.nextInt());
                }

                addRecordFutures.add(getProducer().addUserRecord(stream, partitionKey, ByteBuffer.wrap(baos.toByteArray())));
            }

            // Apply attributes to flow files
            List<FlowFile> failedFlowFiles = new ArrayList<>();
            List<FlowFile> successfulFlowFiles = new ArrayList<>();

            for (int i = 0; i < addRecordFutures.size(); i++) {
                Future<UserRecordResult> future = addRecordFutures.get(i);

                FlowFile flowFile = flowFiles.get(i);
                UserRecordResult userRecordResult;

                try {
                    userRecordResult = future.get();
                }
                catch (ExecutionException ex) {
                    // Handle exception from individual record
                    Throwable cause = ex.getCause();

                    if (cause instanceof UserRecordFailedException) {
                        UserRecordFailedException urfe = (UserRecordFailedException) cause;
                        userRecordResult = urfe.getResult();
                    }
                    else {
                        session.transfer(flowFile, REL_FAILURE);
                        log.error("Failed to publish to Kinesis {} record {}", new Object[]{stream, flowFile});

                        continue;
                    }
                }

                Map<String, String> attributes = createAttributes(userRecordResult);

                flowFile = session.putAllAttributes(flowFile, attributes);

                if (!userRecordResult.isSuccessful()) {
                    failedFlowFiles.add(flowFile);
                }
                else {
                    successfulFlowFiles.add(flowFile);
                }
            }

            if (failedFlowFiles.size() > 0) {
                session.transfer(failedFlowFiles, REL_FAILURE);
                log.error("Failed to publish to {} records to Kinesis {}", new Object[]{failedFlowFiles.size(), stream});
            }

            if (successfulFlowFiles.size() > 0) {
                session.transfer(successfulFlowFiles, REL_SUCCESS);

                if (log.isInfoEnabled()) {
                    log.info("Successfully published {} records to Kinesis {}", new Object[]{successfulFlowFiles, stream});
                }
            }

        }
        catch (final Exception ex) {
            log.error("Failed to publish to Kinesis {} with exception {}", new Object[]{stream, ex});
            session.transfer(flowFiles, REL_FAILURE);
            context.yield();
        }
    }

    /**
     * Helper method to create attributes from {@link com.amazonaws.services.kinesis.producer.UserRecordResult result}
     * object.
     *
     * @param result UserRecordResult
     * @return Map of attributes
     */
    protected Map<String, String> createAttributes(UserRecordResult result) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SUCCESSFUL, Boolean.toString(result.isSuccessful()));
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SHARD_ID, result.getShardId());
        attributes.put(AWS_KINESIS_PRODUCER_RECORD_SEQUENCENUMBER, result.getSequenceNumber());

        int attemptCount = 1;

        for (Attempt attempt : result.getAttempts()) {
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_CODE_SUFFIX, attempt.getErrorCode());
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_DELAY_SUFFIX, Integer.toString(attempt.getDelay()));
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_ERROR_MESSAGE_SUFFIX, attempt.getErrorMessage());
            attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_PREFIX + attemptCount + AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_SUCCESSFUL_SUFFIX,
                    Boolean.toString(attempt.isSuccessful()));

            ++attemptCount;
        }

        attributes.put(AWS_KINESIS_PRODUCER_RECORD_ATTEMPT_COUNT, Integer.toString(result.getAttempts().size()));

        return attributes;
    }

    /**
     * {@inheritDoc}
     */
    @OnShutdown
    public void onShutdown() {
        if (getProducer() != null) {
            getProducer().flushSync();
            getProducer().destroy();
            producer = null;
        }
    }
}