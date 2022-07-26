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
package pl.remekgdansk.kafka.safe.consumer.repository;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import pl.remekgdansk.kafka.safe.model.Notification;

public class OffsetRepository {

    final Map<String, Long> storedOffsets = new ConcurrentHashMap<>();

    public Optional<Long> getOffset(final TopicPartition partition) {
        final String key = partition.topic() + "_" + partition.partition();
        return Optional.ofNullable(storedOffsets.get(key));
    }

    public void storeOffset(final ConsumerRecord<String, Notification> consumerRecord) {
        final String key = consumerRecord.topic() + "_" + consumerRecord.partition();
        storedOffsets.put(key, consumerRecord.offset());
    }
}
