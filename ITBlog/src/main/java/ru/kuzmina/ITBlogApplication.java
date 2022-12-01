package ru.kuzmina;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class ITBlogApplication {
    private static final String QUEUE_NAME = "it_news";
    private static final String EXCHANGER_NAME = "it_news_exchanger";

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        System.out.println("Введите сообщение в формате: тема[пробел]сообщение или stop для остановки приложения");

        Scanner in = new Scanner(System.in);
        String message;

        while (!(message = in.nextLine()).equals("stop")) {
            String messageTheme = message.substring(0, message.indexOf(' '));
            String messageBody = message.substring(message.indexOf(' ') + 1);
            sendMessageToQueue(factory, messageTheme, messageBody);
        }
    }

    private static void sendMessageToQueue(ConnectionFactory factory, String messageTheme, String message) throws IOException, TimeoutException {
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGER_NAME, BuiltinExchangeType.DIRECT);
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGER_NAME, messageTheme);

            channel.basicPublish(EXCHANGER_NAME, messageTheme, null, message.getBytes(StandardCharsets.UTF_8));

            System.out.println("Sent theme[" + messageTheme + "]: " + message);

        }
    }
}
