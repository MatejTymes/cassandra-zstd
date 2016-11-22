package org.apache.cassandra.io.compress;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static com.google.common.primitives.Ints.asList;
import static java.util.UUID.randomUUID;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(Theories.class)
public class ZstdCompressorTest {

    @DataPoints
    public static ZstdCompressor[] compressors = {
            ZstdCompressor.create(Collections.emptyMap()),
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "1")),
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "2")),
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "4")),
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "8")),
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "16"))
    };

    @Theory
    public void shouldBeAbleToCompressAndUncompressBytes(ZstdCompressor compressor) throws IOException {
        String originalText = "This is a random text with random values " + randomUUID();
        byte[] bytes = originalText.getBytes();

        ByteBuffer compressionInput = toDirectByteBuffer(bytes);
        ByteBuffer compressionOutput = toDirectByteBufferOfSize(compressor.initialCompressedBufferLength(bytes.length));

        compressor.compress(compressionInput, compressionOutput);
        byte[] compressedBytes = readBytes(compressionOutput);

        ByteBuffer decompressionInput = toDirectByteBuffer(compressedBytes);
        ByteBuffer decompressionOutput = toDirectByteBufferOfSize(bytes.length);

        compressor.uncompress(decompressionInput, decompressionOutput);
        byte[] decompressedBytes = readBytes(decompressionOutput);

        String decompressedText = new String(decompressedBytes);

        assertThat(decompressedText, equalTo(originalText));

        assertThat(compressionInput.position(), equalTo(compressionInput.limit()));
        assertThat(decompressionInput.position(), equalTo(decompressionInput.limit()));
    }

    @Theory
    public void shouldBeAbleToCompressAndUncompressBytesV2(ZstdCompressor compressor) throws IOException {
        String originalText = "This is a random text with random values " + randomUUID();
        byte[] bytes = originalText.getBytes();

        ByteBuffer compressionInput = toDirectByteBuffer(bytes);
        ByteBuffer compressionOutput = toDirectByteBufferOfSize(compressor.initialCompressedBufferLength(bytes.length));

        compressor.compress(compressionInput, compressionOutput);
        byte[] compressedBytes = readBytes(compressionOutput);

        for (int inputOffset : asList(0, 1, 2, 3)) {
            for (int inputSuffixSize : asList(0, 1, 2, 3)) {
                for (int outputOffset : asList(0, 1, 2, 3)) {

                    byte[] decompressionInput = new byte[compressedBytes.length + inputOffset + inputSuffixSize];
                    System.arraycopy(compressedBytes, 0, decompressionInput, inputOffset, compressedBytes.length);
                    byte[] decompressionOutput = new byte[bytes.length + outputOffset];

                    int size = compressor.uncompress(decompressionInput, inputOffset, compressedBytes.length, decompressionOutput, outputOffset);

                    byte[] decompressedBytes = new byte[bytes.length];
                    System.arraycopy(decompressionOutput, outputOffset, decompressedBytes, 0, bytes.length);
                    String decompressedText = new String(decompressedBytes);

                    assertThat(size, equalTo(bytes.length));
                    assertThat(decompressedText, equalTo(originalText));
                }
            }
        }
    }

    @Theory
    public void shouldProvideSupportedOptions(ZstdCompressor compressor) {
        Set<String> supportedOptions = new HashSet<>();
        supportedOptions.add(ZstdCompressor.COMPRESSION_LEVEL);

        assertThat(compressor.supportedOptions(), equalTo(supportedOptions));
    }

    @Theory
    public void shouldSupportOnlyDirectByteBuffer(ZstdCompressor compressor) {
        assertThat(compressor.supports(BufferType.OFF_HEAP), equalTo(true));
        assertThat(compressor.supports(BufferType.ON_HEAP), equalTo(false));
        assertThat(compressor.preferredBufferType(), equalTo(BufferType.OFF_HEAP));
    }

    @Test
    public void shouldFailIfCompressionLevelHasInvalidValue() {
        try {
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "1.0"));
            fail("expected ConfigurationException");
        } catch (ConfigurationException expected) {
            // this is expected
        }

        try {
            ZstdCompressor.create(toMap(ZstdCompressor.COMPRESSION_LEVEL, "notANumber"));
            fail("expected ConfigurationException");
        } catch (ConfigurationException expected) {
            // this is expected
        }
    }


    private static Map<String, String> toMap(String key, String value) {
        Map<String, String> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    private ByteBuffer toDirectByteBuffer(byte[] inputBytes) {
        ByteBuffer input = ByteBuffer.allocateDirect(inputBytes.length);
        input.put(inputBytes).flip();
        return input;
    }

    private ByteBuffer toDirectByteBufferOfSize(int neededOutputBytes) {
        return ByteBuffer.allocateDirect(neededOutputBytes);
    }

    private byte[] readBytes(ByteBuffer compressionOutput) {
        ByteBuffer buffer = (ByteBuffer) compressionOutput.flip();
        byte[] compressedBytes = new byte[buffer.remaining()];
        buffer.get(compressedBytes);
        return compressedBytes;
    }
}