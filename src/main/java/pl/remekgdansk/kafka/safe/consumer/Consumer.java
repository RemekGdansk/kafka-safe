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

import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import pl.remekgdansk.kafka.safe.consumer.repository.MessageRepository;
import pl.remekgdansk.kafka.safe.consumer.repository.OffsetRepository;
import pl.remekgdansk.kafka.safe.model.Notification;
import pl.remekgdansk.kafka.safe.serialization.ObjectDeserializer;

@Slf4j
public class Consumer {

    private static final String TOPIC = "notifications_safe";

    public static void main(String[] args) {
        final KafkaConsumer<String, Notification> consumer = new KafkaConsumer<>(
            properties(),
            new StringDeserializer(),
            new ObjectDeserializer<>(Notification.class)
        );
        final Thread thread = new Thread(
            new ConsumerRunnable(TOPIC, consumer, new MessageRepository(), new OffsetRepository())
        );
        thread.start();
    }

    private static Properties properties() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091;localhost:9092;localhost:9093");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}
