package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFunction parse;

    public KafkaService(String groupIdName, String topic, ConsumerFunction parse, Class<T> type){
        this.consumer = new KafkaConsumer<>(properties(type, groupIdName));
        consumer.subscribe(Collections.singletonList(topic));
        this.parse = parse;
    }

    public KafkaService(String groupIdName, Pattern topic, ConsumerFunction parse, Class<T> type) {
        this.consumer = new KafkaConsumer<>(properties(type, groupIdName));
        consumer.subscribe(topic);
        this.parse = parse;
    }

    public void run(){
        while(true){
            var records = consumer.poll(Duration.ofMillis(500));

            if(!records.isEmpty()){
                System.out.println("Encontrei "+records.count() +" registros!");
                for(var record: records) {
                    this.parse.consume(record);
                }
            }
        }
    }

    private Properties properties(Class<T> type, String groupIdName){
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getServerAddress());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupIdName);
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());
        return properties;
    }

    private static String getServerAddress(){
        return  "localhost:9092";
    }

    @Override
    public void close() {
        this.consumer.close();
    }
}
