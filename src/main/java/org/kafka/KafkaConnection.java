package org.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaConnection {
    public KafkaProducer<String, Object> createKafkaConnection() {
        Properties properties = new Properties();
        S3ConfigReader s3ConfigReader=new S3ConfigReader();
        Map<String,Object> kafkaConfigMap=s3ConfigReader.populateKafkaConfig();
        System.out.println("kafkaMap value while connection:"+kafkaConfigMap.get("KAFKA_BOOTSTRAP_SERVERS"));
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfigMap.get("KAFKA_BOOTSTRAP_SERVERS").toString());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MessageSerializer.class.getName());
        KafkaProducer<String, Object> producer = new KafkaProducer<String, Object>(properties);
        return producer;
    }

    public void sendData(String topic,Object data) throws InterruptedException, ExecutionException {
        KafkaProducer<String, Object> producer = createKafkaConnection();
        producer.send(new ProducerRecord<String, Object>(topic, data)).get();
        producer.flush();
        producer.close();
    }
}
