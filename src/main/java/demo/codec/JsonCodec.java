package demo.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;
import java.util.function.Function;

public class JsonCodec {

    public static class JsonEncoderFunction implements Function<Map<String, Object>, byte[]> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public byte[] apply(Map<String, Object> jsonMap) {
            try {
                return objectMapper.writeValueAsBytes(jsonMap);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class JsonDecoderFunction implements Function<byte[], Map<String, Object>> {

        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Map<String, Object> apply(byte[] inputBytes) {
            try {
                return objectMapper.readValue(inputBytes, new TypeReference<Map<String,Object>>(){});
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Wraps Processor that gets and returns Map<String, Object> with a JSON encoder/decoder that gets and returns byte array.
     */
    public static Function<byte[], byte[]> over(Function<Map<String, Object>, Map<String, Object>> processor) {
        return inputBytes ->
                new JsonEncoderFunction().apply(
                        processor.apply(
                                new JsonDecoderFunction().apply(inputBytes)
                        )
                );
    }
}
