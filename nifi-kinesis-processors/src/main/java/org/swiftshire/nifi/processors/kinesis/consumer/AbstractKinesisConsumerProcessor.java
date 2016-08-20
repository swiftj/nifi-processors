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

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.*;
import com.amazonaws.http.conn.ssl.SdkTLSSocketFactory;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.metrics.interfaces.MetricsLevel;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnShutdown;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractSessionFactoryProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.aws.credentials.provider.service.AWSCredentialsProviderService;
import org.apache.nifi.ssl.SSLContextService;

import javax.net.ssl.SSLContext;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.nifi.processors.aws.AbstractAWSCredentialsProviderProcessor.*;

/**
 * This class provides processor the base class for Kinesis stream consumer. It declares the
 * property descriptors and supporting methods for the consumer
 *
 */
public abstract class AbstractKinesisConsumerProcessor extends AbstractSessionFactoryProcessor {
    /**
     * The consumer application name
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_APPLICATION_NAME = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Application Name")
            .name("amazon-kinesis-application-name")
            .description("The consumer application name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The consumer worker id prefix.  This prefix is used along with host name and a UUID to generate
     * the final worker id
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_WORKER_ID_PREFIX = new PropertyDescriptor.Builder()
            .displayName("Amazon Kinesis Consumer Worker Id Prefix")
            .name("amazon-kinesis-consumer-worker-id-prefix")
            .description("The Consumer worker id prefix")
            .defaultValue("KinesisConsumerWorkerId")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    /**
     * The starting point in the stream for the consumer
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_INITIAL_POSITION_IN_STREAM = new PropertyDescriptor.Builder()
            .displayName("Initial Position in Stream")
            .name("initial-position-in-stream")
            .description("Initial position in stream from which to start getting events")
            .required(false)
            .defaultValue(InitialPositionInStream.LATEST.name())
            .allowableValues(getInitialPositions())
            .build();

    /**
     * Default time for renewal of lease by a consumer worker
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_FAILOVER_TIME_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Default Failover Time")
            .name("default-failover-time")
            .description("Lease renewal time interval (millis) after which the worker is regarded as failed and lease granted to another worker")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Max records to fetch in each request
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_MAX_RECORDS = new PropertyDescriptor.Builder()
            .displayName("Max Records in Each Request")
            .name("max-records-in-each-request")
            .description("Maximum number of records to be fetched in each request from the Kinesis stream")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Idle time between record reads
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_IDLETIME_BETWEEN_READS_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Idle Time Betweeen Record Fetch")
            .name("idle-time-betweeen-record-fetch")
            .description("Idle time between record reads (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("1000")
            .build();

    /**
     * Skip empty records call to records processor
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_DONT_CALL_PROCESS_RECORDS_FOR_EMPTY_RECORD_LIST = new PropertyDescriptor.Builder()
            .displayName("Skip call if records list is empty record")
            .name("skip-call-if-records-list-is-empty")
            .description("Don't call record processor if record list is empty")
            .required(false)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .defaultValue("true")
            .build();

    /**
     * Polling interval for parent shard
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_PARENT_SHARD_POLL_INTERVAL_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Parent Shard Poll Interval")
            .name("parent-shard-poll-interval")
            .description("Interval between polling to check for parent shard completion (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Sync shard interval
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_SHARD_SYNC_INTERVAL_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Shard Sync Interval")
            .name("shard-sync-interval")
            .description("Sync interval for shard tasks (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("60000")
            .build();

    /**
     * Clean up lease after shard completion
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_CLEANUP_LEASES_UPON_SHARDS_COMPLETION = new PropertyDescriptor.Builder()
            .displayName("Clean up Lease on Shard Completion")
            .name("clean-up-lease-on-shard-completion")
            .description("Proactively clean up leases to reduce resource tracking")
            .required(false)
            .allowableValues(new AllowableValue("true"), new AllowableValue("false"))
            .defaultValue("true")
            .build();

    /**
     * Back off time interval in case of failures
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_TASK_BACKOFF_TIME_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Back Off Time on Failure")
            .name("back-off-time-on-failure")
            .description("Backoff time interval on failure (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("500")
            .build();

    /**
     * Metrics buffer interval in millis
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_BUFFER_TIME_MILLIS = new PropertyDescriptor.Builder()
            .displayName("Max Metrics Buffer interval")
            .name("max-metrics-buffer-interval")
            .description("Interval for which metrics are buffered (millis)")
            .required(false)
            .addValidator(StandardValidators.LONG_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Buffer metrics max count
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_MAX_QUEUE_SIZE = new PropertyDescriptor.Builder()
            .displayName("Max Metrics Buffer Count")
            .name("max-metrics-buffer-count")
            .description("Buffer max count for metrics")
            .required(false)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .defaultValue("10000")
            .build();

    /**
     * Metrics level
     */
    public static final PropertyDescriptor KINESIS_CONSUMER_DEFAULT_METRICS_LEVEL = new PropertyDescriptor.Builder()
            .displayName("Metrics Level")
            .name("metrics-level")
            .description("Level of metrics send to AWS CloudWatch")
            .required(false)
            .allowableValues(getMetricsAllowableValues())
            .defaultValue(MetricsLevel.DETAILED.name())
            .build();

    /**
     * The procession session factory reference
     */
    private final AtomicReference<ProcessSessionFactory> sessionFactoryReference = new AtomicReference<>();

    /**
     * Our main KCL library reference.
     */
    protected volatile AmazonKinesisClient client;

    /**
     * AWS region we're targeting
     */
    protected volatile Region region;

    /**
     * Our HTTP user agent header we send on all our KCL requests.
     */
    protected static final String DEFAULT_USER_AGENT = "Apache NiFi";

    /**
     * Invoked by Nifi which initializes and configures our internal KCL client for use with
     * AWS Kinesis Streams.
     */
    @SuppressWarnings("unused")
    @OnScheduled
    public void onScheduled(ProcessContext context) {
        ControllerService service = context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService();
        final ComponentLog log = getLogger();

        if (service != null) {
            if (log.isDebugEnabled()) {
                log.debug("Using aws credentials provider service for creating client");
            }

            this.client = new AmazonKinesisClient(getCredentialsProvider(context), createConfiguration(context));
        }
        else {
            if (log.isDebugEnabled()) {
                log.debug("Using aws credentials for creating client");
            }

            this.client = new AmazonKinesisClient(getCredentials(context), createConfiguration(context));
        }

        intializeRegionAndEndpoint(context);
    }

    /**
     * Called by Nifi to shutdown this processor. We, in turn, shutdown the KCL.
     */
    @SuppressWarnings("unused")
    @OnShutdown
    public void onShutDown() {
        if (this.client != null) {
            this.client.shutdown();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> problems = new ArrayList<>(super.customValidate(validationContext));

        final boolean accessKeySet = validationContext.getProperty(ACCESS_KEY).isSet();
        final boolean secretKeySet = validationContext.getProperty(SECRET_KEY).isSet();

        if ((accessKeySet && !secretKeySet) || (secretKeySet && !accessKeySet)) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation(
                    "If setting Secret Key or Access Key, must set both").build());
        }

        final boolean credentialsFileSet = validationContext.getProperty(CREDENTIALS_FILE).isSet();

        if ((secretKeySet || accessKeySet) && credentialsFileSet) {
            problems.add(new ValidationResult.Builder().input("Access Key").valid(false).explanation(
                    "Cannot set both Credentials File and Secret Key/Access Key").build());
        }

        final boolean proxyHostSet = validationContext.getProperty(PROXY_HOST).isSet();
        final boolean proxyHostPortSet = validationContext.getProperty(PROXY_HOST_PORT).isSet();

        if (((!proxyHostSet) && proxyHostPortSet) || (proxyHostSet && (!proxyHostPortSet))) {
            problems.add(new ValidationResult.Builder().input("Proxy Host Port").valid(false).explanation(
                    "Both proxy host and port must be set").build());
        }

        return problems;
    }

    /**
     * Get credentials provider using the {@link AWSCredentialsProviderService}
     *
     * @param context the process context
     * @return AWSCredentialsProvider the credential provider
     * @see <a href="http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/auth/AWSCredentialsProvider.html">AWSCredentialsProvider</a>
     */
    protected AWSCredentialsProvider getCredentialsProvider(final ProcessContext context) {

        final AWSCredentialsProviderService credentialsService =
                context.getProperty(AWS_CREDENTIALS_PROVIDER_SERVICE).asControllerService(AWSCredentialsProviderService.class);

        return credentialsService.getCredentialsProvider();
    }

    /**
     * Creates and initializes our KCL configuration to use.
     *
     * @param context
     * @return
     */
    protected ClientConfiguration createConfiguration(final ProcessContext context) {
        final ClientConfiguration config = new ClientConfiguration();

        config.setMaxConnections(context.getMaxConcurrentTasks());
        config.setMaxErrorRetry(0);
        config.setUserAgent(DEFAULT_USER_AGENT);

        final int timeout = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        config.setConnectionTimeout(timeout);
        config.setSocketTimeout(timeout);

        final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);

        if (sslContextService != null) {
            final SSLContext sslContext = sslContextService.createSSLContext(SSLContextService.ClientAuth.NONE);

            SdkTLSSocketFactory sdkTLSSocketFactory = new SdkTLSSocketFactory(sslContext, null);
            config.getApacheHttpClientConfig().setSslSocketFactory(sdkTLSSocketFactory);
        }

        if (context.getProperty(PROXY_HOST).isSet()) {
            String proxyHost = context.getProperty(PROXY_HOST).getValue();
            config.setProxyHost(proxyHost);

            Integer proxyPort = context.getProperty(PROXY_HOST_PORT).asInteger();
            config.setProxyPort(proxyPort);
        }

        return config;
    }

    /**
     *
     * @param context
     */
    protected void intializeRegionAndEndpoint(ProcessContext context) {
        // If the processor supports REGION, get the configured region.
        if (getSupportedPropertyDescriptors().contains(REGION)) {
            final String regionName = context.getProperty(REGION).getValue();

            if (regionName != null) {
                region = Region.getRegion(Regions.fromName(regionName));
                client.setRegion(region);
            }
            else {
                region = null;
            }
        }

        // If the endpoint override has been configured, set the endpoint.
        // (per Amazon docs this should only be configured at client creation)
        final String endpoint = StringUtils.trimToEmpty(context.getProperty(ENDPOINT_OVERRIDE).getValue());

        if (!endpoint.isEmpty()) {
            client.setEndpoint(endpoint);
        }
    }

    /**
     * Returns the KCL client used by this Nifi Processor
     *
     * @return KCL client reference
     */
    protected AmazonKinesisClient getClient() {
        return client;
    }

    /**
     * Returns the AWS {@link Region region} object used by this Nifi Processor
     *
     * @return Region we're configured for
     */
    protected Region getRegion() {
        return region;
    }

    /**
     * Returns the AWS access keys we're configured to use. If none are configured, we use the standard
     * anonymous credentials by default. We currently support properties defined in Nifi or we can load
     * them from a separate properties file.
     * <p/>
     * We may want to consider other types of {@link AWSCredentials credentials} in the future.
     *
     * @param context
     * @return The AWS credentials to use when accessing the cloud
     */
    protected AWSCredentials getCredentials(final ProcessContext context) {
        final String accessKey = context.getProperty(ACCESS_KEY).evaluateAttributeExpressions().getValue();
        final String secretKey = context.getProperty(SECRET_KEY).evaluateAttributeExpressions().getValue();

        final String credentialsFile = context.getProperty(CREDENTIALS_FILE).getValue();

        if (credentialsFile != null) {
            try {
                return new PropertiesCredentials(new File(credentialsFile));
            }
            catch (final IOException ex) {
                throw new ProcessException("Could not read Credentials File", ex);
            }
        }

        if (accessKey != null && secretKey != null) {
            return new BasicAWSCredentials(accessKey, secretKey);
        }

        return new AnonymousAWSCredentials();
    }

    /**
     * Shuts down the active KCL client being used to connect to and work with AWS Kinesis.
     */
    @OnShutdown
    public void onShutdown() {
        if (getClient() != null) {
            getClient().shutdown();
        }
    }

    /**
     *
     * @param regions
     * @return
     */
    private static AllowableValue createAllowableValue(final Regions regions) {
        return new AllowableValue(regions.getName(), regions.getName(), regions.getName());
    }

    /**
     *
     * @return
     */
    private static AllowableValue[] getAvailableRegions() {
        final List<AllowableValue> values = new ArrayList<>();

        for (final Regions regions : Regions.values()) {
            values.add(createAllowableValue(regions));
        }

        return values.toArray(new AllowableValue[values.size()]);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onTrigger(ProcessContext context, ProcessSessionFactory sessionFactory) throws ProcessException {
        sessionFactoryReference.compareAndSet(null, sessionFactory);
        context.yield();
    }

    /**
     * Get the metrics levels for reporting to AWS
     *
     * @return metric levels
     */
    protected static Set<String> getMetricsAllowableValues() {
        Set<String> values = new HashSet<>();

        for (MetricsLevel ml : MetricsLevel.values()) {
            values.add(ml.name());
        }

        return values;
    }

    /**
     * Get the initial positions options to indicate where to start the stream
     *
     * @return initial position options
     */
    protected static Set<String> getInitialPositions() {
        Set<String> values = new HashSet<>();

        for (InitialPositionInStream position : InitialPositionInStream.values()) {
            values.add(position.name());
        }

        return values;
    }

    /**
     * Get reference to ProcessSessionFactory
     *
     * @return the process session factory
     */
    protected ProcessSessionFactory getSessionFactory() {
        return sessionFactoryReference.get();
    }
}