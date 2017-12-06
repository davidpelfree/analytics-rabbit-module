package demo.transport;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

public class RabbitMQ {


    public static class Context {
        public final Connection connection;
        public final Channel channel;
        public final String host;

        public Context(String rabbitMqServerHost) throws IOException, TimeoutException {
            host = rabbitMqServerHost;
            final ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(rabbitMqServerHost);
            connection = factory.newConnection();
            channel = connection.createChannel();
        }

        public void shutdown() {
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
    }

    public static class Reader {
        private final Context context;
        private final String inQueue;
        private final Function<byte[], byte[]> processor;
        private final java.util.function.Consumer<byte[]> recordWriter;
        private static int terminatingTimeoutSeconds = 10; // terminate processSingleRecord if no message received after timeout
        private volatile boolean shouldShutDown = false;

        public Reader(
                String inQueue,
                Context context,
                Function<byte[], byte[]> processor,
                java.util.function.Consumer<byte[]> recordWriter
        ) {
            this.context = context;
            this.inQueue = inQueue;
            this.processor = processor;
            this.recordWriter = recordWriter;
        }

        /**
         * Listens to RabbiMQ specified input queue until queue is cancelled / deleted.
         */
        public void run() throws IOException {
            try {
                context.channel.queueDeclare(inQueue, false, false, false, null);

                final com.rabbitmq.client.Consumer consumer = new DefaultConsumer(context.channel) {
                    @Override
                    public void handleCancelOk(String consumerTag) {
                        shouldShutDown = true;
                    }

                    @Override
                    public void handleCancel(String consumerTag) throws IOException {
                        shouldShutDown = true;
                    }

                    @Override
                    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {
                        shouldShutDown = true;
                    }

                    @Override
                    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                        final byte[] outputRecordBytes = processor.apply(body);
                        recordWriter.accept(outputRecordBytes);
                    }
                };
                context.channel.basicConsume(inQueue, true, consumer);

                while (!shouldShutDown) {
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ignored) {
                    }
                }

                System.err.println("Input queue '" + inQueue + "' cancelled. Shutting down");
            } finally {
                try {
                    context.shutdown();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static class Writer implements Consumer<byte[]> {

        private final String outQueue;
        private final Channel channel;

        public Writer(String outQueue, Context context) throws IOException {
            this.outQueue = outQueue;
            this.channel = context.channel;
            channel.queueDeclare(outQueue, false, false, false, null);
        }

        @Override
        public void accept(byte[] singleRecordOutputBytes) {
            try {
                channel.basicPublish("", outQueue, null, singleRecordOutputBytes);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Manual test method.
     */
    public static void main(String... args) throws Exception {

        final Function<byte[], byte[]> processor = in -> {
            // Add "#" to each message.
            return ("#" + new String(in)).getBytes();
        };

        final Context context = new Context("localhost");
        final java.util.function.Consumer<byte[]> recordWriter = new Writer("out", context);
        final Reader me = new Reader("in", context, processor, recordWriter);
        me.run();
    }
}
