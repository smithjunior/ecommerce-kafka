package  br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());

        System.out.println("Started send messages to kafka ");
        for(int index = 0; index <100; index++){
            producerNewMessage(producer);
        }
        System.out.println("Finished send messages to kafka");
    }

    private static void producerNewMessage(KafkaProducer<String, String> producer) throws InterruptedException, ExecutionException {
        var keyOrder = UUID.randomUUID().toString();
        var order = "{ \"pedido_id\": "+ keyOrder + ", \"preco\": "+radomNummber()+" }";

        var email = "Welcome! We are processing your order";

        var orderRecord  = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", keyOrder, order);
        var emailRecord  = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", keyOrder, email);

        Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
            }
            System.out.println(data.topic());
        };

        producer.send(orderRecord, callback).get();
        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getServerAddress());
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    private static Integer radomNummber(){
        int min = 500;
        int max = 5000;

        //Generate random int value from 50 to 100
        return (int)Math.floor(Math.random()*(max-min+1)+min);
    }

    private static String getServerAddress(){
        return  "localhost:9092";
    }
}
