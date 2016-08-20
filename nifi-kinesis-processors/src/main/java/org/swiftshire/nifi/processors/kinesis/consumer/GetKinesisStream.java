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

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.KinesisClientLibDependencyException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.clientlibrary.types.InitializationInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ProcessRecordsInput;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownInput;
import com.amazonaws.services.kinesis.model.Record;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.util.StopWatch;

import java.io.ByteArrayInputStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.*;
import static org.swiftshire.nifi.processors.kinesis.KinesisPropertyDescriptors.*;

@SupportsBatching
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"amazon", "aws", "kinesis", "get", "stream"})
@CapabilityDescription("Get the records from the specified Amazon Kinesis stream. "
        + " This Kinesis processor uses the AWS Kinesis Client Library which automatically uses DynamoDB to store client state and CloudWatch to store metrics."
        + " AWS credentials used by this processor must have permissions to access to those AWS services."
        + " Consequently, use of this processor may incur unexpected AWS account costs."
)
@WritesAttributes({
        @WritesAttribute(attribute = GetKinesisStream.AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP, description = "Approximate arrival time of the record"),
        @WritesAttribute(attribute = GetKinesisStream.AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY, description = "Partition key of the record"),
        @WritesAttribute(attribute = GetKinesisStream.AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER, description = "Sequence number of the record"),
        @WritesAttribute(attribute = GetKinesisStream.AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND, description = "Consumer lag for processing records"),
        @WritesAttribute(attribute = GetKinesisStream.KINESIS_CONSUMER_RECORD_START_TIMESTAMP, description = "Timestamp when the particular batch of records was processed "),
        @WritesAttribute(attribute = GetKinesisStream.KINESIS_CONSUMER_RECORD_NUMBER, description = "Record number of the record processed in that batch")
})
public class GetKinesisStream extends AbstractKinesisConsumerProcessor implements RecordsHandler {
    /**
     * Attributes written by processor
     */
    public static final String AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY = "aws.kinesis.consumer.record.partition.key";
    public static final String AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER = "aws.kinesis.consumer.record.sequence.number";
    public static final String AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP = "aws.kinesis.consumer.record.approx.arrival.timestamp";
    public static final String AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND = "aws.kinesis.consumer.record.milli.seconds.behind";
    public static final String KINESIS_CONSUMER_RECORD_START_TIMESTAMP = "kinesis.consumer.record.start.timestamp";
    public static final String KINESIS_CONSUMER_RECORD_NUMBER = "kinesis.consumer.record.number";

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(Arrays.asList(REGION,
            AWS_CREDENTIALS_PROVIDER_SERVICE, KINESIS_STREAM_NAME, KINESIS_CONSUMER_APPLICATION_NAME,
            KINESIS_CONSUMER_WORKER_ID_PREFIX, BATCH_SIZE, KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM,
            KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS, KINESIS_CONSUMER_DEFAULT_MAX_RECORDS,
            KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS, KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST,
            KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS, KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS,
            KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION,
            KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS, KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS,
            KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE, KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL));

    /**
     *
     */
    public static final Set<Relationship> relationshipsGetKinesisStream = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS)));

    /**
     * Our internal thread pool to process Kinesis records
     */
    final protected ExecutorService executor = Executors.newCachedThreadPool();

    /**
     * KCL Worker that we use to consume records
     */
    protected Worker consumerWorker;

    /**
     * Configurable batch size of records to process
     */
    protected int batchSize;

    /**
     * Configurable name of the Kinesis stream we're reading from
     */
    private String streamName;

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<Relationship> getRelationships() {
        return relationshipsGetKinesisStream;
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
    public void onScheduled(final ProcessContext context) throws ProcessException {

        final AWSCredentialsProviderService awsCredentialsProviderService = getAWSCredentialsProviderService(context);

        streamName = context.getProperty(KINESIS_STREAM_NAME).getValue();
        KinesisClientLibConfiguration config;

        try {
            config = new KinesisClientLibConfiguration(
                    context.getProperty(KINESIS_CONSUMER_APPLICATION_NAME).getValue(),
                    context.getProperty(KINESIS_STREAM_NAME).getValue(),
                    awsCredentialsProviderService.getCredentialsProvider(),
                    context.getProperty(KINESIS_CONSUMER_WORKER_ID_PREFIX).getValue() + ":"
                            + InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID().toString())
                    .withRegionName(context.getProperty(REGION).getValue())
                    .withInitialPositionInStream(
                            InitialPositionInStream.valueOf(
                                    context.getProperty(KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM).getValue()))
                    .withFailoverTimeMillis(
                            context.getProperty(KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS).asLong())
                    .withMaxRecords(context.getProperty(KINESIS_CONSUMER_DEFAULT_MAX_RECORDS).asInteger())
                    .withIdleTimeBetweenReadsInMillis(context.getProperty(KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS).asLong())
                    .withCallProcessRecordsEvenForEmptyRecordList(!context.getProperty(KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST).asBoolean())
                    .withParentShardPollIntervalMillis(context.getProperty(KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS).asLong())
                    .withShardSyncIntervalMillis(context.getProperty(KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS).asLong())
                    .withCleanupLeasesUponShardCompletion(context.getProperty(KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION).asBoolean())
                    .withTaskBackoffTimeMillis(context.getProperty(KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS).asLong())
                    .withMetricsBufferTimeMillis(context.getProperty(KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS).asLong())
                    .withMetricsMaxQueueSize(context.getProperty(KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE).asInteger())
                    .withMetricsLevel(context.getProperty(KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL).getValue());

            batchSize = context.getProperty(BATCH_SIZE).asInteger();
            config.withMaxRecords(batchSize);

            KinesisRecordProcessorFactory kinesisRecordProcessorFactory = new KinesisRecordProcessorFactory(this);

            consumerWorker = makeWorker(config, kinesisRecordProcessorFactory);

            executor.execute(consumerWorker);
        }
        catch (Exception ex) {
            throw new ProcessException(ex);
        }
    }

    /**
     * Helper method to get credentials service
     *
     * @param context the process context
     * @return return aws creds provider service
     */
    protected AWSCredentialsProviderService getAWSCredentialsProviderService(final ProcessContext context) {
        return context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE)
                .asControllerService(AWSCredentialsProviderService.class);
    }

    /**
     * Helper method to create worker
     *
     * @param config                        kinesis config
     * @param kinesisRecordProcessorFactory record processor factory
     * @return return the worker
     */
    protected Worker makeWorker(KinesisClientLibConfiguration config, KinesisRecordProcessorFactory kinesisRecordProcessorFactory) {
        return new Worker
                .Builder()
                .recordProcessorFactory(kinesisRecordProcessorFactory)
                .config(config)
                .build();
    }

    /**
     * Shuts down our worker and thread pool
     */
    @OnShutdown
    public void onShutdown() {
        if (consumerWorker != null) {
            consumerWorker.shutdown();
        }

        executor.shutdownNow();

        super.onShutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize(InitializationInput initializationInput) {
        final ComponentLog log = getLogger();

        if (log.isDebugEnabled()) {
            log.debug("Intializing : " + initializationInput.getShardId() + ":" + initializationInput.getExtendedSequenceNumber());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput, InitializationInput initializationInput) {
        Record lastRecordProcessed = null;
        int processedRecords = 0;
        long timestamp = System.currentTimeMillis();

        FlowFile flowFile = null;
        ProcessSession session = getSessionFactory().createSession();

        try {
            for (Record record : processRecordsInput.getRecords()) {
                try {
                    flowFile = session.create();
                    StopWatch stopWatch = new StopWatch(true);
                    ByteArrayInputStream baos = new ByteArrayInputStream(record.getData().array());
                    flowFile = session.importFrom(baos, flowFile);

                    Map<String, String> attributes = createAttributes(
                            processRecordsInput, processedRecords, timestamp, record);

                    flowFile = session.putAllAttributes(flowFile, attributes);

                    session.transfer(flowFile, REL_SUCCESS);

                    session.getProvenanceReporter().receive(flowFile,
                            "kinesis://" + streamName + "/" + timestamp + "/" + ++processedRecords,
                            + stopWatch.getElapsed(TimeUnit.MILLISECONDS));

                    lastRecordProcessed = record;
                }
                catch (Exception ex) {
                    if (flowFile != null) {
                        session.remove(flowFile);
                    }

                    getLogger().error("Error while handling record: " + record + " with exception: " + ex.getMessage());
                }
            }
        }
        finally {
            try {
                if (lastRecordProcessed != null) {
                    processRecordsInput.getCheckpointer().checkpoint(lastRecordProcessed);
                }
                else {
                    processRecordsInput.getCheckpointer().checkpoint();
                }
            }
            catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException ex) {
                getLogger().error("Exception while checkpointing record " + ex.getMessage());
            }
        }

        session.commit();
    }

    /**
     * Creates our collections of configuration attributes.
     *
     * @param processRecordsInput
     * @param processedRecords
     * @param timestamp
     * @param record
     * @return
     */
    protected Map<String, String> createAttributes(
            ProcessRecordsInput processRecordsInput, int processedRecords, long timestamp, Record record) {

        Map<String, String> attributes = new HashMap<>();

        attributes.put(AWS_KINESIS_CONSUMER_RECORD_PARTITION_KEY, record.getPartitionKey());
        attributes.put(AWS_KINESIS_CONSUMER_RECORD_SEQUENCE_NUMBER, record.getSequenceNumber());
        attributes.put(AWS_KINESIS_CONSUMER_MILLIS_SECONDS_BEHIND,
                Long.toString(processRecordsInput.getMillisBehindLatest()));
        attributes.put(AWS_KINESIS_CONSUMER_RECORD_APPROX_ARRIVAL_TIMESTAMP,
                Long.toString(record.getApproximateArrivalTimestamp().getTime()));
        attributes.put(KINESIS_CONSUMER_RECORD_START_TIMESTAMP, Long.toString(timestamp));
        attributes.put(KINESIS_CONSUMER_RECORD_NUMBER, Integer.toString(processedRecords));

        return attributes;
    }

    /**
     * Record processor shutting down
     *
     * @param shutdownInput the shutdown input object
     * @param initializationInput
     */
    @Override
    public void shutdown(ShutdownInput shutdownInput, InitializationInput initializationInput) {
        final ComponentLog log = getLogger();

        if (log.isDebugEnabled()) {
            log.debug("Shutdown : " + shutdownInput.getShutdownReason() + " intializationInput " +
                    initializationInput.getShardId() + ":" + initializationInput.getExtendedSequenceNumber());
        }

        try {
            shutdownInput.getCheckpointer().checkpoint();
        }
        catch (KinesisClientLibDependencyException | InvalidStateException | ThrottlingException | ShutdownException ignore) {
            log.error("Exception while shutting down processor " + shutdownInput.getShutdownReason()
                    + " with intiaitlization input " + initializationInput.getShardId() + ":" +
                    initializationInput.getExtendedSequenceNumber());
        }
    }
}