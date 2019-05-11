package pl.designuj.projects.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * created by designuj on 10/05/2019
 */
public class SeekConsumer {
    private final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String bootstrapServer;
    private final String topic;

    public SeekConsumer(String bootstrapServer, String topic) {
        this.bootstrapServer = bootstrapServer;
        this.topic = topic;
    }

    private Properties consumerProperties(String bootstrapServer) {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    private void setupConsumer(KafkaConsumer<String, String> consumer, long offset, int partition) {
        TopicPartition topicPartition = new TopicPartition(topic, partition);

        consumer.assign(Collections.singletonList(topicPartition));
        consumer.seek(topicPartition, offset);
    }

    private void fetchMessages(KafkaConsumer<String, String> consumer, int quantity) {
        int numberOfMessagesRead = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead += 1;

                logger.info("Key: " + record.key() + ", value: " + record.value());
                logger.info("Partition: " + record.partition() + ", fffset: " + record.offset());

                if (numberOfMessagesRead >= quantity) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }

    public void run(long offset, int partition, int quantity) {
        Properties properties = consumerProperties(bootstrapServer);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        setupConsumer(consumer, offset, partition);
        fetchMessages(consumer, quantity);
    }

    public static void main(String[] args) {
        String server = "127.01.0.1:9092";
        String topic = "user_registered";

        long offset = 15L;
        int partition = 0;
        int quantity = 5;

        new SeekConsumer(server, topic).run(offset, partition, quantity);
    }
}
