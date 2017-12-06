package demo;

import demo.support.XpathBasedParser;
import demo.transport.RabbitMQ;

import java.io.IOException;
import java.util.function.Consumer;

public final class DemoUadParserWriteToRabbitMQ {
    static boolean isFirst = true; // TODO Not thread safe. Fix

    public static void main(String... args) throws Exception {
        ProducerMixin producerMixin = ProducerMixin.init(args);

        if (producerMixin.isConsoleMode) {
            System.err.print("Program started. ");
            System.err.println("Writing to console.");

            parse(in -> {
                if (!isFirst) System.out.write('\0');
                try {
                    System.out.write(in);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                isFirst = false;
            });

        } else {
            final RabbitMQ.Context rabbitContext = new RabbitMQ.Context("localhost");
            try {
                final RabbitMQ.Writer rabbitMQWriter = new RabbitMQ.Writer(producerMixin.outQueue, rabbitContext);

                System.err.print("Program started. ");
                System.err.println("Writing to rabbitMQ: " + producerMixin.outQueue + "@" + rabbitContext.host + ".");

                parse(rabbitMQWriter);

            } finally {
                try {
                    rabbitContext.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        System.err.println("Program ended");
    }

    private static void parse(Consumer<byte[]> writer) throws Exception {
        XpathBasedParser.parseFromSystemIn(writer::accept);
    }
}
