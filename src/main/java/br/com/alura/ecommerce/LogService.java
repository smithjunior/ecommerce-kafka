package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.time.Duration;
import java.util.Collections;
import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var logService = new LogService();
        try(var kafkaService = new KafkaService(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
            logService::parse
        )){
            kafkaService.run();
        }
//        consumer.subscribe(Collections.singletonList("ECOMMERCE.*"));
//        while(true){
//            var records = consumer.poll(Duration.ofMillis(500));
//
//            if(!records.isEmpty()){
//                System.out.println("Encontrei "+records.count() +" registros!");
//                for(var record: records) {
//
//                }
//            }
//
//        }
    }

    private void parse(ConsumerRecord<String, String> record){
        System.out.println("----");
        System.out.println("LOG: " + record.topic());
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
    }
}
