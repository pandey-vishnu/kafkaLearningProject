package tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    public static void main(String[] args) {
        System.out.println("hello world!!");
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());
        String bootStratServer = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStratServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);


        //Subscribe Consumer to out topics
        consumer.subscribe(Arrays.asList("topic"));

        //Poll for new data
        while (true){
          ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(100)); // inroduced in kafka 2
            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: " + record.key() + " , value: ", record.value());
                logger.info("Partition: "+ record.partition() + "Offset:" + record.offset());
            }
        }


    }}