package org.kafka;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class S3ConfigReader {
    public static String profile=System.getenv("profile");
    public  static String fileLocation="kafka-"+profile+"-config.properties";
    public static String bucketName="xyz";


    public Map<String, Object> populateKafkaConfig() {

        AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withRegion(Regions.AP_SOUTH_1).build();
        System.out.println("fileName :"+fileLocation);
        // Read the file content from S3
        S3Object s3Object = s3Client.getObject(bucketName, fileLocation);
        Map<String, Object> kafkaConfig = new HashMap<>();
        try (S3ObjectInputStream s3InputStream = s3Object.getObjectContent()) {
            Properties properties = new Properties();
            properties.load(s3InputStream);
                kafkaConfig.put("KAFKA_BOOTSTRAP_SERVERS",properties.getProperty("kafka.bootstrap.servers"));
                System.out.println("kafka-bootstrap-server"+kafkaConfig.get("KAFKA_BOOTSTRAP_SERVERS"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return kafkaConfig;
    }
}
