package demo;

import java.util.function.Function;

public class DemoHelloProcessorFromConsoleOrRabbit extends ProcessorSkeleton {


    public static void main(String... args) throws Exception {
        DemoHelloProcessorFromConsoleOrRabbit me = new DemoHelloProcessorFromConsoleOrRabbit();
        me.mainSkeleton(args);
    }

    @Override
    protected Function<byte[], byte[]> processor() {
        return in -> ("Hello " + new String(in)).getBytes();
    }
}
