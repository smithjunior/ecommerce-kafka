package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));
        while(true){
            var records = consumer.poll(Duration.ofMillis(500));

            if(!records.isEmpty()){
                System.out.println("Encontrei "+records.count() +" registros!");
                for(var record: records) {
                    System.out.println("----");
                    System.out.println("Processando novo pedido, checkando por fraude!");
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    System.out.println("----");
                    try {
                        Thread.sleep(5000);
                    }catch(InterruptedException e) {
                        e.printStackTrace();
                    }
                    System.out.println("Pedido processado");
                }
            }

        }

    }

    private static Properties properties(){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getServerAddress());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, FraudDetectorService.class.getName()+"_"+ UUID.randomUUID().toString());
        return properties;
    }

    private static String getServerAddress(){
        return  "localhost:9092";
    }
}
