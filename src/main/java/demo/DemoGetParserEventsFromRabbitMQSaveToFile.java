package demo;

import demo.transport.RabbitMQ;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DemoGetParserEventsFromRabbitMQSaveToFile {

    private static String inQueue;
    private static String baseDir;

    public static void main(String... args) throws Exception {
        parseArgs(args);

        final RabbitMQ.Context rabbitContext = new RabbitMQ.Context("localhost");
        new File(baseDir).mkdirs();

        System.err.print("Program started. ");
        System.err.println("Consuming from rabbitMQ: " + inQueue + "@" + rabbitContext.host + " . Writing to base dir: " + baseDir + ".");
        System.err.println("Program will not stop until you break it with ^C.");

        final java.util.function.Consumer<byte[]> writer = singleRecordBytes -> {

            // Parse header line - look for '\n'
            final int indexOfNewLine = ((Function<byte[], Integer>) bytes -> {
                for (int i = 0; i < bytes.length; i++) {
                    if (bytes[i] == '\n') return i;
                }
                return -1;
            }).apply(singleRecordBytes);

            if (indexOfNewLine > 0) {

                // We found header line containing comma separated list of "key:value"
                final byte[] headerBytes = Arrays.copyOf(singleRecordBytes, indexOfNewLine - 1);

                // Convert header line to Map
                final Map<String, String> headers = ((Function<byte[], Map<String, String>>) bytes ->
                        Arrays.stream(new String(bytes).split(","))
                                .map(kv -> kv.split(":"))
                                .collect(Collectors.toMap(
                                        (String[] kv) -> kv[0], kv -> kv[1]
                                ))
                ).apply(headerBytes);

                final String type = headers.get("type");
                final String id = headers.get("id");
                final String shortId = id.substring(0, 2);

                final File file = new File(baseDir, type + "/" + shortId + "/" + id + "/content.xml");
                file.getParentFile().mkdirs();

                final String startOfContent = new String(singleRecordBytes, indexOfNewLine + 1, Math.min(singleRecordBytes.length - indexOfNewLine - 1, 30));
                System.out.println("Writing to file: " + file + " Content: " + startOfContent);

                try (FileOutputStream fos = new FileOutputStream(file)) {
                    fos.write(singleRecordBytes);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            } else {
                System.err.println("Record ignored: missing header line");
            }
        };

        final RabbitMQ.Reader rabbitMQReader = new RabbitMQ.Reader(inQueue, rabbitContext, Function.identity(), writer);
        rabbitMQReader.run();

        System.err.println("Program ended");
    }

    private static void parseArgs(String... args) throws Exception {
        try {
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--in-queue":
                        final String arg2 = args[++i];
                        inQueue = arg2;
                        break;
                    case "--base-dir":
                        final String arg3 = args[++i];
                        baseDir = arg3;
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (inQueue == null || baseDir == null) {
            throw new IllegalArgumentException("Usage: --in-queue <rabbitMQ-input-queue> --base-dir <base-dir-for-event-storage>");
        }
    }
}
