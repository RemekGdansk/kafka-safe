/*
 * MIT License
 *
 * Copyright (c) 2022 RemekGdansk
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package pl.remekgdansk.kafka.safe.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.remekgdansk.kafka.safe.consumer.repository.MessageRepository;
import pl.remekgdansk.kafka.safe.consumer.repository.OffsetRepository;
import pl.remekgdansk.kafka.safe.model.Notification;

@Slf4j
@RequiredArgsConstructor
public class ConsumerRunnable implements Runnable {

    private static final Duration POLL_TIMEOUT = Duration.parse("PT10S");

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final String topic;
    private final KafkaConsumer<String, Notification> consumer;

    private final MessageRepository messageRepository;
    private final OffsetRepository offsetRepository;

    @Override
    public void run() {
        final RebalanceListener rebalanceListener = new RebalanceListener(consumer, offsetRepository);
        try {
            consumer.subscribe(List.of(topic), rebalanceListener);
            consumer.poll(POLL_TIMEOUT);
            seekToOffsets();
            processMessages(rebalanceListener);
        } catch (WakeupException e) {
            // Ignore exception if closing
            if (!closed.get()) throw e;
        } finally {
            try {
                log.info("Attempting commit sync");
                final Map<TopicPartition, OffsetAndMetadata> currentOffsets = rebalanceListener.getCurrentOffsets();
                consumer.commitSync(currentOffsets);
                log.info("Sync commit successful with offsets {}", currentOffsets);
            } catch (RuntimeException e) {
                log.warn("Sync commit failed");
            } finally {
                log.info("Close consumer");
                consumer.close();
            }
        }
    }

    public void shutdown() {
        closed.set(true);
        consumer.wakeup();
    }

    private void seekToOffsets() {
        consumer
            .assignment()
            .forEach(partition -> {
                final Optional<Long> offset = offsetRepository.getOffset(partition);
                if (offset.isPresent()) {
                    long start = offset.get() + 1;
                    consumer.seek(partition, start);
                    log.info("Offset for partition {} set to {}.", partition, start);
                }
            });
    }

    private void processMessages(final RebalanceListener rebalanceListener) {
        while (!closed.get()) {
            final ConsumerRecords<String, Notification> records = consumer.poll(POLL_TIMEOUT);
            for (ConsumerRecord<String, Notification> record : records) {
                processMessage(record, rebalanceListener);
            }
        }
    }

    private void processMessage(
        final ConsumerRecord<String, Notification> record,
        final RebalanceListener rebalanceListener
    ) {
        log.info("Received message: {}", record);
        final UUID messageId = record.value().uuid();
        if (messageRepository.isMessageAlreadyProcessed(messageId)) {
            log.warn("Message with messageId {} was processed before, skipping processing", messageId);
        } else {
            messageRepository.saveMessageIdAsProcessed(messageId);
            log.info("Processed message: {}", record.value());
        }
        rebalanceListener.addOffset(record.topic(), record.partition(), record.offset());
        offsetRepository.storeOffset(record);
        consumer.commitAsync(
            Map.of(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset(), "processed")
            ),
            new MyOffsetCommitCallback()
        );
    }

    private static class MyOffsetCommitCallback implements OffsetCommitCallback {

        @Override
        public void onComplete(final Map<TopicPartition, OffsetAndMetadata> offsets, final Exception exception) {
            if (Objects.nonNull(exception)) {
                log.error("Commit failed for offsets {} due to exception", offsets, exception);
            } else {
                log.info("Async commit successful with offsets {}", offsets);
            }
        }
    }
}
