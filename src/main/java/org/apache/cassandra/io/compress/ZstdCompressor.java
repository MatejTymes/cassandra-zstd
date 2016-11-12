package org.apache.cassandra.io.compress;

import com.github.luben.zstd.Zstd;
import org.apache.cassandra.exceptions.ConfigurationException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * ZStandard ICompressor for Cassandra
 *
 * @author Matej Tymes
 */
public class ZstdCompressor implements ICompressor {

    public static final String COMPRESSION_LEVEL = "compression_level";
    public static final Integer DEFAULT_COMPRESSION_LEVEL = 1;

    private final int level;

    public static ZstdCompressor create(Map<String, String> options) {
        // no specific options supported so far
        return new ZstdCompressor(parseCompressionLevel(options));
    }

    private ZstdCompressor(int level) {
        this.level = level;
    }

    @Override
    public int initialCompressedBufferLength(int chunkLength) {
        return (int) Zstd.compressBound(chunkLength);
    }

    @Override
    public int uncompress(byte[] input, int inputOffset, int inputLength, byte[] output, int outputOffset) throws IOException {
        ByteBuffer inputBuffer = ByteBuffer.allocateDirect(inputLength);
        inputBuffer.put(input, inputOffset, inputLength).flip();
        ByteBuffer outputBuffer = ByteBuffer.allocateDirect(output.length - outputOffset);
        uncompress(inputBuffer, outputBuffer);

        ByteBuffer buffer = (ByteBuffer) outputBuffer.flip();
        int size = buffer.remaining();
        buffer.get(output, outputOffset, size);

        return size;
    }

    @Override
    public void compress(ByteBuffer input, ByteBuffer output) throws IOException {
        long size = Zstd.compressDirectByteBuffer(
                output,
                output.position(),
                output.remaining(),
                input,
                input.position(),
                input.limit() - input.position(),
                level);
        output.position((int) size);
    }

    @Override
    public void uncompress(ByteBuffer input, ByteBuffer output) throws IOException {
        Zstd.decompressDirectByteBuffer(
                output,
                output.position(),
                output.remaining(),
                input,
                input.position(),
                input.limit() - input.position()
        );
        output.position(output.limit());
    }

    @Override
    public BufferType preferredBufferType() {
        return BufferType.OFF_HEAP;
    }

    @Override
    public boolean supports(BufferType bufferType) {
        // can only support direct buffer
        return BufferType.OFF_HEAP == bufferType;
    }

    @Override
    public Set<String> supportedOptions() {
        return new HashSet<>(Arrays.asList(COMPRESSION_LEVEL));
    }

    private static Integer parseCompressionLevel(Map<String, String> options) {
        String compressionLevel = options.get(COMPRESSION_LEVEL);

        if (compressionLevel == null) {
            return DEFAULT_COMPRESSION_LEVEL;
        }

        Integer level;
        try {
            level = Integer.parseInt(compressionLevel);
        } catch (NumberFormatException e) {
            throw new ConfigurationException(String.format("Unable to read '%s' as '%s' an is invalid value", COMPRESSION_LEVEL, compressionLevel), e);
        }
        return level;
    }
}
