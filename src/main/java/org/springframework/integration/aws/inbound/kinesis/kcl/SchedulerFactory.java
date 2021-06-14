package org.springframework.integration.aws.inbound.kinesis.kcl;

import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public interface SchedulerFactory {
    Scheduler create(ShardRecordProcessorFactory shardRecordProcessorFactory);
}
