package com.phonepe;

import com.phonepe.api.Consumer;
import com.phonepe.api.Message;
import com.phonepe.api.MessageCondition;
import com.phonepe.api.MessageQueue;
import com.phonepe.api.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class InMemoryMessageQueue implements MessageQueue {

    private final ConcurrentMap<MessageCondition, List<DependentConsumer>> subscribers;

    private final Queue<Record> messages;

    private DLQHandler dlqHandler;

    private final int size;
    private final ExecutorService executorService;
    private final int maxRetries = 3;
    private final List<Future> submittedTasks = new ArrayList<>();

    public InMemoryMessageQueue(int size, int maxWorkerThreads) {
        this.size = size;
        this.executorService = Executors.newFixedThreadPool(maxWorkerThreads);
        this.messages = new ConcurrentLinkedQueue<>();
        this.subscribers = new ConcurrentHashMap<>();
    }

    @Override
    public void notify(Record record) throws QueueOverflowException {
        addIfQueueCanProcess(record);
        process();
    }

    @Override
    public void subscribe(MessageCondition messageCondition, Consumer consumer, Consumer... dependentOn) {
        final DependentConsumer dependentConsumer = new DependentConsumer(consumer, dependentOn);
        final List<DependentConsumer> dependentConsumers = subscribers.putIfAbsent(messageCondition,
                new CopyOnWriteArrayList<>(Collections.singleton(dependentConsumer)));
        if (dependentConsumers != null) {
            dependentConsumers.add(dependentConsumer);
        }
    }

    /**
     * Blocking call, which waits until all pending tasks are complete
     */
    public void waitUntilAllPendingTaskCompletes() throws Exception {
        for (int i = 0; i < submittedTasks.size(); i++) {
            submittedTasks.get(i).get();
        }
    }

    private DependentConsumer findConsumerIn(List<DependentConsumer> consumers, Consumer consumer) {
        final int index = consumers.indexOf(new DependentConsumer(consumer, null));
        if (index != -1) {
            return consumers.get(index);
        } else {
            return null;
        }
    }

    private void process() {
        final Future<?> future = this.executorService.submit(() -> {
            final Optional<Message> optionalMessage = pollMessages();

            optionalMessage.ifPresent(message -> {
                subscribers.forEach((condition, consumers) -> {
                    if (condition.test(message)) {
                        final Stack<DependentConsumer> consumersToProcess = new Stack<>();
                        consumers.forEach(consumersToProcess::push);

                        while (!consumersToProcess.isEmpty()) {
                            final DependentConsumer consumer = consumersToProcess.peek();
                            if (consumer.hasDependents()) {
                                boolean allProcessed = true;

                                for (Consumer dependency : consumer.dependentOn) {
                                    final DependentConsumer dependentConsumer = findConsumerIn(consumers, dependency);
                                    if (dependentConsumer != null && !dependentConsumer.processed) {
                                        allProcessed = false;
                                        if (consumersToProcess.indexOf(dependentConsumer) != -1) {
                                            consumersToProcess.remove(dependentConsumer);
                                        }
                                        consumersToProcess.push(dependentConsumer);
                                    }
                                }

                                if (allProcessed) {
                                    consumer.tryProcess(message);
                                    consumersToProcess.pop();
                                }
                            } else {
                                consumer.tryProcess(message);
                                consumersToProcess.pop();
                            }
                        }
                    }
                });
            });
        });
        submittedTasks.add(future);
    }

    private Optional<Message> pollMessages() {
        while (!messages.isEmpty()) {
            if (!this.messages.peek().isMessageExpired()) {
                return Optional.ofNullable(this.messages.poll().getMessage());
            } else {
                this.messages.poll();
            }
        }
        return Optional.empty();
    }

    private void addIfQueueCanProcess(Record record) throws QueueOverflowException {
        if (messages.size() <= size) {
            messages.offer(record);
        } else {
            throw new QueueOverflowException("Queue is full");
        }
    }

    private class DependentConsumer {
        private final Consumer consumer;
        private final Consumer[] dependentOn;
        private boolean processed;
        private int numberOfTries;

        public DependentConsumer(Consumer consumer, Consumer[] dependentOn) {
            this.consumer = consumer;
            this.dependentOn = dependentOn;
            this.processed = false;
            this.numberOfTries = 0;
        }

        public boolean hasDependents() {
            return dependentOn.length != 0;
        }

        public void tryProcess(Message message) {
            while (!processed) {
                try {
                    consumer.process(message);
                    processed = true;
                } catch (Exception e) {
                    numberOfTries++;
                    if (numberOfTries >= maxRetries) {
                        dlqHandler.handleError(new Record(message));
                    }
                }
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            DependentConsumer that = (DependentConsumer) o;
            return Objects.equals(consumer, that.consumer);
        }

        @Override
        public int hashCode() {
            return Objects.hash(consumer);
        }
    }


}
