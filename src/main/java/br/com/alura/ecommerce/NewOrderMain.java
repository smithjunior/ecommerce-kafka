package  br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(
        var kafkaOrderDispatcher = new KafkaDispatcher<Order>();
        var kafkaEmailDispatcher = new KafkaDispatcher<String>()
        ) {

            System.out.println("Started send messages to kafka ");

            for(int index = 0; index <100; index++){
                var keyOrder = UUID.randomUUID().toString();

                var order = new Order(keyOrder, radomNummber());
                producerNewOrderMessage(kafkaOrderDispatcher, keyOrder, order);

                var email = "Welcome! We are processing your order";
                producerNewEmailMessage(kafkaEmailDispatcher, keyOrder, email);

            }
            System.out.println("Finished send messages to kafka");
        }
    }

    private static void producerNewOrderMessage(KafkaDispatcher kafkaDispatcher, String keyOrder, Order order) throws InterruptedException, ExecutionException {
        kafkaDispatcher.send("ECOMMERCE_NEW_ORDER", keyOrder, order);
    }

    private static void producerNewEmailMessage(KafkaDispatcher kafkaDispatcher, String keyOrder, String email) throws ExecutionException, InterruptedException {
        kafkaDispatcher.send("ECOMMERCE_SEND_EMAIL", keyOrder, email);
    }

    private static Double radomNummber(){
        int min = 500;
        int max = 5000;

        //Generate random int value from 50 to 100
        return Math.floor(Math.random()*(max-min+1)+min);
    }

}
