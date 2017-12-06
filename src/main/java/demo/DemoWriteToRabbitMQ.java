package demo;

import demo.transport.RabbitMQ;

public class DemoWriteToRabbitMQ {
    private static String outQueue;

    public static void main(String... args) throws Exception {
        parseArgs(args);

        final RabbitMQ.Context rabbitContext = new RabbitMQ.Context("localhost");
        final RabbitMQ.Writer rabbitMQWriter = new RabbitMQ.Writer(outQueue, rabbitContext);

        System.err.print("Program started. ");
        System.err.println("Writing to rabbitMQ: " + outQueue + "@" + rabbitContext.host + ".");

        for (int i = 0; i < 10; i++) {
            final String msg = "Message #" + i;
            System.err.println("Sending to RabbitMQ: " + msg);
            rabbitMQWriter.accept(msg.getBytes());
            Thread.sleep(100);
        }

        System.err.println("Program ended");
        System.exit(0);
    }

    private static void parseArgs(String... args) throws Exception {
        try {
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--out-queue":
                        final String arg3 = args[++i];
                        outQueue = arg3;
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (outQueue == null) {
            throw new IllegalArgumentException("Usage: --out-queue <rabbitMQ-output-queue>");
        }
    }
}
