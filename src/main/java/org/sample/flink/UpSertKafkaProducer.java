package org.sample.flink;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class UpSertKafkaProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> kafkaProducer = new KafkaProducer<>(props);
        for (int i = 0; i < 1; i++) {
            String key = "{\"user_id\":\"1\"}";
            String value = "{\"user_id\":\"1\", \"page_id\":\"90000\",\"viewtime\":\"2021-07-01 00:00:02\",\"user_region\":\"US\"}";
            //String value = null;
            ProducerRecord<String, String> record = new ProducerRecord<>("pageviews",
                    key, value
            );
            Future<RecordMetadata> future = kafkaProducer.send(record);
            Thread.sleep(5000);
        }
    }
}
