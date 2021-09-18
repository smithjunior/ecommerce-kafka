package br.com.alura.ecommerce;

public class Order {
    private String id;
    private Double value;

    public Order(String id, Double value) {
        this.id = id;
        this.value = value;
    }

    @Override
    public String toString() {
        return "Order{" +
                "id=" + id +
                ", value=" + value +
                '}';
    }
}
