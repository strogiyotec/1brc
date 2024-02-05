package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import sun.misc.Unsafe;

public final class CalculateAverage_almas {

    private static final String FILE_PATH = "./mes2.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    private static Unsafe initUnsafe() {
        try {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(final String[] args) throws IOException {
        final FileChannel channel = new RandomAccessFile(FILE_PATH, "r").getChannel();
        final List<Chunk> chunks = CalculateAverage_almas.computeChunks(channel, Runtime.getRuntime().availableProcessors());
        for (final Chunk chunk : chunks) {
            for (long i = chunk.start; i < chunk.length() + chunk.start(); i++) {
                System.out.print((char) UNSAFE.getByte(i));
            }
        }
    }

    private static List<Chunk> computeChunks(final FileChannel channel, final int threads) throws IOException {
        final long fileSize = channel.size();
        final long estimatedChunkSize = fileSize / threads;
        final List<Chunk> chunks = new ArrayList<>(threads);
        final long mappedAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize, Arena.global()).address();
        long chunkStart = 0L;
        long chunkEnd = Math.min(fileSize - 1, estimatedChunkSize);
        while (chunkStart < fileSize) {
            final long startPosition = chunkStart + chunkEnd;
            final MappedByteBuffer buffer = channel.map(
                FileChannel.MapMode.READ_ONLY,
                startPosition,
                Math.min(
                    fileSize - startPosition, estimatedChunkSize
                )
            );
            while (buffer.get() != (byte) '\n') {
                chunkEnd++;
            }
            //add a new line
            chunkEnd++;
            chunks.add(
                new Chunk(
                    chunkStart + mappedAddress,
                    chunkEnd
                )
            );
            chunkStart += chunkEnd;
            chunkEnd = Math.min(estimatedChunkSize, fileSize - chunkStart - 1L/*leave a space for an empty line*/);
        }
        return chunks;
    }

    private record Chunk(long start, long length) {

    }

}
