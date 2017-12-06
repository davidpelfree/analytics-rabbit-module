package demo;

/**
 * A skeleton for a process that write events to console or to messaging queue.
 * To be mixed in and called by a class with main().
 */
public final class ProducerMixin {

    public final String outQueue;
    public final String rabbitMqServerHost;
    public final boolean isConsoleMode;

    private ProducerMixin(String outQueue, String rabbitMqServerHost, boolean isConsoleMode) {
        this.outQueue = outQueue;
        this.rabbitMqServerHost = rabbitMqServerHost;
        this.isConsoleMode = isConsoleMode;
    }

    public static ProducerMixin init(String... args) throws Exception {
        boolean isConsoleMode = false;
        String outQueue = null;
        try {
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--console":
                        isConsoleMode = true;
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

        if (!isConsoleMode && outQueue == null) {
            throw new IllegalArgumentException("Usage: --console  or: --out-queue <rabbitMQ-output-queue>");
        }
        return new ProducerMixin(outQueue, "localhost", isConsoleMode);
    }
}
