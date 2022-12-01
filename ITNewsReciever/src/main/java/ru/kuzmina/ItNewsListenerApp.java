package ru.kuzmina;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ItNewsListenerApp {
    private static final String EXCHANGER_NAME = "it_news_exchanger";
    private static Channel channel;
    private static String queue;
    private static Connection connection;

    public static void main(String[] args) throws IOException, TimeoutException {
        initConnection();

        System.out.println("Добавьте тему рассылки командой: set_topic[пробел]название темы");
        System.out.println("Удалить тему рассылки командой: delete_topic[пробел]название темы");
        System.out.println("Остановить приложение командой: stop");

        listenChannel();
        closeConnection();
    }

    private static void listenChannel() throws IOException {
        Scanner in = new Scanner(System.in);
        String command;

        while (!(command = in.nextLine()).equals("stop")) {
            if (command.startsWith("set_topic")) {
                String topic = command.split("set_topic ")[1];
                System.out.println("Добавлена тема для получения новостей: " + topic);
                channel.queueBind(queue, EXCHANGER_NAME, topic);
            }
            if (command.startsWith("delete_topic")) {
                String topic = command.split("delete_topic ")[1];
                System.out.println("Удалена тема для получения новостей: " + topic);
                channel.queueUnbind(queue, EXCHANGER_NAME, topic);
            }

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                String routingKey = delivery.getEnvelope().getRoutingKey();
                System.out.format("[%s]: %s", routingKey, message).println();
            };

            channel.basicConsume(queue, true, deliverCallback, consumerTag -> {
            });
        }
    }

    private static void closeConnection() throws IOException, TimeoutException {
        channel.queueDelete(queue);
        channel.close();
        connection.close();
    }

    private static void initConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        connection = factory.newConnection();
        channel = connection.createChannel();
        queue = channel.queueDeclare().getQueue();
        channel.queueBind(queue, EXCHANGER_NAME, "");
    }
}
