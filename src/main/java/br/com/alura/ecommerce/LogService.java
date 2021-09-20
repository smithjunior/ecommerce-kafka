package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public class LogService {
    public static void main(String[] args) {
        var logService = new LogService();
        try(var kafkaService = new KafkaService<String>(
                LogService.class.getSimpleName(),
                Pattern.compile("ECOMMERCE.*"),
            logService::parse,
            String.class
        )){
            kafkaService.run();
        }

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
