package org.sample.flink;

import com.alibaba.fastjson2.JSON;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;

public class KafkaProducerTest {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 10000;
        for (int i = 0; i < totalMessageCount; i++) {
//            String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
            Order order=new Order();
            order.userId="user-"+i;
            order.orderId="order-"+i;
            Date now=new Date();
            order.eventTime= DateFormatUtils.format(DateUtils.addMinutes(now,-1),"yyyy-MM-dd'T'HH:mm:ss'Z'");
            order.userName="user-"+i;
            order.priceAmount=100.0;
            String value= JSON.toJSONString(order);
            producer.send(new ProducerRecord<>("flinktopic", value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(1000L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}

