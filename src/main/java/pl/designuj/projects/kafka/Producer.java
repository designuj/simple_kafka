package pl.designuj.projects.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * created by designuj on 10/05/2019
 */
public class Producer {
    private final Logger logger = LoggerFactory.getLogger(Producer.class);
    private final KafkaProducer<String, String> producer;

    public Producer(String bootstrapServer) {
        Properties properties = producerProperties(bootstrapServer);
        producer = new KafkaProducer<>(properties);
        logger.info("Producer started");
    }

    private Properties producerProperties(String bootstrapServer) {
        String serializer = StringSerializer.class.getName();
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        return properties;
    }

    public void put(String topic, String key, String value) throws ExecutionException, InterruptedException {
        logger.info("Put value: " + value + " for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, ((recordMetadata, e) -> {
            if (e != null) {
                logger.error("Error while producing", e);
            }

            logger.info("Received new data. \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        })).get();
    }

    public void close() {
        logger.info("Closing connection with producer");
        producer.close();
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "John");
        producer.put(topic, "user2", "Kate");
    }
}
