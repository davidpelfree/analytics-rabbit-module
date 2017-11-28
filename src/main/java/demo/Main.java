package demo;

import com.rabbitmq.client.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.TimeoutException;

public class Main {

    private static String inQueue;
    private static String outQueue;
    private static String rabbitMqServerHost = "localhost";
    private static boolean isConsoleMode = false;

    public static void main(String... args) throws Exception {

        parseArgs(args);

        System.err.print("Program started. ");
        if (isConsoleMode) {
            System.err.println("Consuming from stdin. Writing to stdout.");
            System.err.println("Program will not stop until you close stdin (using ^D/^Z) or break it with ^C.");
        } else {
            System.err.println("Consuming from rabbitMQ: " + inQueue + "@" + rabbitMqServerHost + " . Writing to rabbitMQ:" + outQueue + "@" + rabbitMqServerHost + ".");
            System.err.println("Program will not stop until you break it with ^C.");
        }

        if (isConsoleMode) {
            processConsole();
        } else {
            final Channel channel = initRabbitMQ();
            channel.queueDeclare(outQueue, false, false, false, null);
            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    processRabbitMQ(channel, process(message));
                }
            };
            channel.basicConsume(inQueue, true, consumer);
        }

        System.err.println("Program ended");
    }

    private static Channel initRabbitMQ() throws IOException, TimeoutException {
        final ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbitMqServerHost);
        final Connection connection = factory.newConnection();
        final Channel channel = connection.createChannel();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    channel.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        return channel;
    }

    private static void processRabbitMQ(Channel channel, String message) throws IOException {
        channel.basicPublish("", outQueue, null, message.getBytes());
    }

    private static void processConsole() throws IOException {
        String line;
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(System.in));
            while ((line = reader.readLine()) != null) {
                System.out.println(process(line));
            }
        } finally {
            if (reader != null) reader.close();
        }
    }

    private static String process(String line) {
        return "Hello " + line;
    }

    private static void parseArgs(String... args) throws Exception {
        try {
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--console":
                        isConsoleMode = true;
                        break;
                    case "--in-queue":
                        final String arg2 = args[++i];
                        inQueue = arg2;
                        break;
                    case "--out-queue":
                        final String arg3 = args[++i];
                        outQueue = arg3;
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!isConsoleMode && (inQueue == null || outQueue == null)) {
            throw new IllegalArgumentException("Usage: --console  or: --in-queue <rabbitMQ-input-queue> --out-queue <rabbitMQ-output-queue>");
        }
    }
}
