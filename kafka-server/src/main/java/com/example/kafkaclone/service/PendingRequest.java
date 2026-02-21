package com.example.kafkaclone.service;

import com.example.kafka.api.ConsumerResponse;
import io.grpc.stub.StreamObserver;
import lombok.Builder;

import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Builder
public class PendingRequest {
    StreamObserver<ConsumerResponse> responseStreamObserver;
    long requestedOffset;
    ScheduledFuture<?> timeoutTask;
    private final AtomicBoolean isDone = new AtomicBoolean(false);

    public PendingRequest(StreamObserver<ConsumerResponse> responseStreamObserver, long requestedOffset, ScheduledFuture<?> timeoutTask) {
        this.responseStreamObserver = responseStreamObserver;
        this.requestedOffset = requestedOffset;
        this.timeoutTask = timeoutTask;
    }

    public boolean tryComplete(ConsumerResponse response) {
        if (isDone.compareAndSet(false, true)) {
            try {
                this.responseStreamObserver.onNext(response);
                this.responseStreamObserver.onCompleted();
            } catch (Exception e) {
                // In case of disconnect
                throw new RuntimeException(e);
            }

            return true;
        }

        return false;
    }
}
