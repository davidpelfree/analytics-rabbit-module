package demo;

import demo.transport.Console;
import demo.transport.RabbitMQ;

import java.util.function.Function;

public abstract class ProcessorSkeleton {

    private static String inQueue;
    private static String outQueue;
    private static String rabbitMqServerHost = "localhost";
    private static boolean isConsoleMode = false;

    protected final void mainSkeleton(String... args) throws Exception {
        parseArgs(args);

        System.err.print("Program started. ");
        if (isConsoleMode) {
            System.err.println("Consuming from stdin. Writing to stdout.");
            System.err.println("Program will not stop until you close stdin (using ^D/^Z) or break it with ^C.");

            new Console.Reader('\n', processor(), new Console.Writer('\n'))
                    .run();

        } else {
            System.err.println("Consuming from rabbitMQ: " + inQueue + "@" + rabbitMqServerHost + " . Writing to rabbitMQ: " + outQueue + "@" + rabbitMqServerHost + ".");
            System.err.println("Program will stop when input queue is deleted or when you break it with ^C.");

            final RabbitMQ.Context rabbitMQContext = new RabbitMQ.Context(rabbitMqServerHost);
            new RabbitMQ.Reader(inQueue, rabbitMQContext, processor(), new RabbitMQ.Writer(outQueue, rabbitMQContext))
                    .run();
        }
    }

    /**
     * Actual processor logic. To be implemented by subclass.
     */
    protected abstract Function<byte[], byte[]> processor() ;

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
