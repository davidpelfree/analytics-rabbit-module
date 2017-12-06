package demo;

/**
 * A skeleton for a process that write events to console or to messaging queue.
 * To be mixed in and called by a class with main().
 */
public final class ConsumerMixin {

    public final String inQueue;
    public final String rabbitMqServerHost;
    public final boolean isConsoleMode;

    private ConsumerMixin(String inQueue, String rabbitMqServerHost, boolean isConsoleMode) {
        this.inQueue = inQueue;
        this.rabbitMqServerHost = rabbitMqServerHost;
        this.isConsoleMode = isConsoleMode;
    }

    public static ConsumerMixin init(String... args) throws Exception {
        boolean isConsoleMode = false;
        String inQueue = null;
        try {
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--console":
                        isConsoleMode = true;
                        break;
                    case "--in-queue":
                        final String arg3 = args[++i];
                        inQueue = arg3;
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (!isConsoleMode && inQueue == null) {
            throw new IllegalArgumentException("Usage: --console  or: --in-queue <rabbitMQ-input-queue>");
        }
        return new ConsumerMixin(inQueue, "localhost", isConsoleMode);
    }
}
