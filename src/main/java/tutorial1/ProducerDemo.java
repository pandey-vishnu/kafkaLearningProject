package tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWIthCallback.class);

        String bootstarpServer = "127.0.0.1:9092";
        /* create producer properties */
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstarpServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        //Record to send
        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>("first_topic", "hum honge kamyaab");



//flush data
        producer.flush();
        //flush and close the producer
        producer.close();





    }
}