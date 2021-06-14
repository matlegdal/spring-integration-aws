package org.springframework.integration.aws.inbound.kinesis.kcl;


import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.support.management.IntegrationManagedResource;
import org.springframework.jmx.export.annotation.ManagedResource;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.exceptions.InvalidStateException;
import software.amazon.kinesis.exceptions.ShutdownException;
import software.amazon.kinesis.lifecycle.events.InitializationInput;
import software.amazon.kinesis.lifecycle.events.LeaseLostInput;
import software.amazon.kinesis.lifecycle.events.ProcessRecordsInput;
import software.amazon.kinesis.lifecycle.events.ShardEndedInput;
import software.amazon.kinesis.lifecycle.events.ShutdownRequestedInput;
import software.amazon.kinesis.processor.ShardRecordProcessor;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@ManagedResource
@IntegrationManagedResource
public class KclV2MessageDrivenChannelAdapter extends MessageProducerSupport {
    private final Scheduler scheduler;
    private final TaskExecutor executor;

    public KclV2MessageDrivenChannelAdapter(SchedulerFactory schedulerFactory) {
        this(schedulerFactory, new SimpleAsyncTaskExecutor());
    }

    public KclV2MessageDrivenChannelAdapter(SchedulerFactory schedulerFactory, TaskExecutor executor) {
        this.scheduler = schedulerFactory.create(MessageShardRecordProcessor::new);
        this.executor = executor;
    }

    public List<String> getStreamNames() {
        return scheduler.retrievalConfig().appStreamTracker().map(
                multiStreamTracker -> multiStreamTracker.streamConfigList().stream()
                        .map(streamConfig -> streamConfig.streamIdentifier().streamName())
                        .collect(Collectors.toList()),
                streamConfig ->
                        Collections.singletonList(streamConfig.streamIdentifier().streamName()));
    }

    public String getConsumerGroup() {
        return scheduler.applicationName();
    }

    public String getWorkerId() {
        return scheduler.leaseManagementConfig().workerIdentifier();
    }

    @Override
    protected void doStart() {
        logger.debug(() -> "Start kcl channel adapter on streams " + getStreamNames() + " for consumer " + getConsumerGroup() + " with worker id " + getWorkerId());
        executor.execute(scheduler);
    }

    @Override
    protected void doStop() {
        logger.debug(() -> "Stop kcl channel adapter on streams " + getStreamNames() + " for consumer " + getConsumerGroup());
        scheduler.startGracefulShutdown();
    }

    @Override
    public void destroy() {
        logger.debug(() -> "Destroy kcl channel adapter on streams " + getStreamNames() + " for consumer " + getConsumerGroup());
        super.destroy();
        if (super.isRunning()) {
            scheduler.shutdown();
        }
    }

    private class MessageShardRecordProcessor implements ShardRecordProcessor {
        private String shardId;

        @Override
        public void initialize(InitializationInput initializationInput) {
            shardId = initializationInput.shardId();
            logger.info(() -> "Initializing record processor for " + getStreamNames() + "/" + shardId + " @ Sequence: " + initializationInput.extendedSequenceNumber());
        }

        @Override
        public void processRecords(ProcessRecordsInput processRecordsInput) {

        }

        @Override
        public void leaseLost(LeaseLostInput leaseLostInput) {
            logger.info(() -> "Lost lease on " + getStreamNames() + "/" + shardId + ", so terminating.");
        }

        @Override
        public void shardEnded(ShardEndedInput shardEndedInput) {
            logger.info(() -> "Reached shard end of " + getStreamNames() + "/" + shardId + ", checkpointing...");
            try {
                shardEndedInput.checkpointer().checkpoint();
            } catch (InvalidStateException | ShutdownException e) {
                logger.error(e, "Exception while checkpointing at shard end. Giving up");
            }
        }

        @Override
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            logger.info(() -> "Shutdown requested for " + getStreamNames() + "/" + shardId + ", checkpointing...");
            try {
                shutdownRequestedInput.checkpointer().checkpoint();
            } catch (InvalidStateException | ShutdownException e) {
                logger.error(e, "Exception while checkpointing at shutdown requested. Giving up");
            }
        }
    }
}
