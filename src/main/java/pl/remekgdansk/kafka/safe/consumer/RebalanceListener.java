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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import pl.remekgdansk.kafka.safe.consumer.repository.OffsetRepository;
import pl.remekgdansk.kafka.safe.model.Notification;

@Slf4j
@RequiredArgsConstructor
public class RebalanceListener implements ConsumerRebalanceListener {

    private final KafkaConsumer<String, Notification> consumer;

    private final OffsetRepository offsetRepository;

    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();

    public void addOffset(final String topic, final int partition, final long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    @Override
    public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        log.info("Partitions assigned. Seeking offset.");
        partitions.forEach(partition -> {
            final Optional<Long> offsetOptional = offsetRepository.getOffset(partition);
            if (offsetOptional.isPresent()) {
                final Long offset = offsetOptional.get();
                consumer.seek(partition, offset);
                log.info("Offset for partition {} set to {}", partition, offset);
            }
        });
    }

    @Override
    public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}
}
