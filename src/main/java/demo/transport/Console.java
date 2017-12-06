package demo.transport;

import demo.codec.JsonCodec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

public class Console {

    /**
     * Read from console (System.in) and push to the supplier processor, then write to supplied RecordWriter.
     */
    public static class Reader {

        public static final int BUFFER_SIZE = 4096;

        private final int inputRecordSeparator;
        private final Function<byte[], byte[]> processor;
        private final Consumer<byte[]> recordWriter;
        private boolean isFirstRecord = true;

        public Reader(int inputRecordSeparator, Function<byte[], byte[]> processor, Consumer<byte[]> recordWriter) {
            this.inputRecordSeparator = inputRecordSeparator;
            this.processor = processor;
            this.recordWriter = recordWriter;
        }

        /**
         * Listens to System.in (stdin) until EOF (Ctrl-D in Linux, Ctrl-Z in Windows).
         * Separate records by new-line (Line-Feed / \n / ASCII-10) or any other configurable single character (e.g. ASCII-0).
         */
        public void run() throws IOException {
            final ByteArrayOutputStream singleRecordInputBytes = new ByteArrayOutputStream();
            final byte[] buffer = new byte[BUFFER_SIZE];
            while (true) {
                int startOfRecord = 0;
                final int bytesRead = System.in.read(buffer);
                if (bytesRead < 0) {
                    // we're at end-of-file / end-of-stream
                    if (singleRecordInputBytes.size() > 0) {
                        processSingleRecord(singleRecordInputBytes.toByteArray());
                    }
                    break;
                }

                int endOfRecord = searchRecordSeparator(buffer, startOfRecord, bytesRead);
                while (endOfRecord >= 0) {
                    // found end-of-record in buffer
                    singleRecordInputBytes.write(buffer, startOfRecord, endOfRecord - startOfRecord);
                    startOfRecord = endOfRecord + 1;
                    processSingleRecord(singleRecordInputBytes.toByteArray());
                    singleRecordInputBytes.reset();
                    endOfRecord = searchRecordSeparator(buffer, startOfRecord, bytesRead);
                }
                int len = bytesRead - startOfRecord;
                if (len > 0) singleRecordInputBytes.write(buffer, startOfRecord, len);
            }
        }

        private int searchRecordSeparator(byte[] buffer, int startIndex, int bytesRead) {
            for (int i = startIndex; i < bytesRead; i++) {
                if (buffer[i] == inputRecordSeparator) return i;
            }
            return -1;
        }

        private void processSingleRecord(byte[] singleRecordInputBytes) throws IOException {
            try {
                final byte[] result = processor.apply(singleRecordInputBytes);
                recordWriter.accept(result);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class Writer implements Consumer<byte[]> {

        private final int outputRecordSeparator;
        private boolean isFirstRecord = true;

        public Writer(int outputRecordSeparator) {
            this.outputRecordSeparator = outputRecordSeparator;
        }

        @Override
        public void accept(byte[] singleRecordOutputBytes) {
            try {
                if (!isFirstRecord) System.out.write(outputRecordSeparator);
                System.out.write(singleRecordOutputBytes);
            } catch (Exception e) {
                e.printStackTrace();
            }
            isFirstRecord = false;
        }
    }

    /**
     * Manual test method.
     */
    public static void main(String... args) throws Exception {

        System.err.println("Supply valid JSON and press Enter.");
        System.err.println("Press Ctrl-D to finish");

        final Function<byte[], byte[]> simpleProcessor = in -> {
            // Add "#" to each message. No codec required here.
            return ("#" + new String(in)).getBytes();
        };

        final Function<byte[], byte[]> jsonProcessor = JsonCodec.over(map -> {
            map.put("validated", Boolean.TRUE);
            return map;
        });

        final Reader me = new Reader(
                '\n',
                jsonProcessor,
                new Writer('\n')
        );

        me.run();
    }

}
