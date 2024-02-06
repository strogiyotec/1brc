package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import sun.misc.Unsafe;

public final class CalculateAverage_almas {

    private static final String FILE_PATH = "./mes2.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    private static final ConcurrentLinkedQueue<ResultRow> RESULTS = new ConcurrentLinkedQueue();

    private static final int CITY_NAME_MAX_LENGTH = 100;

    private static final int MAP_SIZE = 10_000;

    private static final int FNV_32_INIT = 0x811c9dc5;
    private static final int FNV_32_PRIME = 16777619;

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
        CalculateAverage_almas.computeChunks(channel, Runtime.getRuntime().availableProcessors());
    }

    private static void computeChunks(final FileChannel channel, final int threads) throws IOException {
        final long fileSize = channel.size();
        final long estimatedChunkSize = fileSize / threads;
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
            final Chunk chunk = new Chunk(
                chunkStart + mappedAddress,
                chunkEnd
            );
            CalculateAverage_almas.parseChunk(chunk);
            chunkStart += chunkEnd;
            chunkEnd = Math.min(estimatedChunkSize, fileSize - chunkStart - 1L/*leave a space for an empty line*/);
        }
    }

    private static void parseChunk(final Chunk chunk) {
        final LinearProbingMap map = new LinearProbingMap();
        //allocate in once and then reuse
        final byte[] name = new byte[CITY_NAME_MAX_LENGTH];
        long addressIndex = chunk.start();
        final long chunkLength = chunk.start() + chunk.length();
        while (addressIndex < chunkLength) {
            int nameIndex = 0;
            int nameHash = FNV_32_INIT;
            byte fileByte;
            //compute the hash
            while ((fileByte = UNSAFE.getByte(addressIndex)) != (byte) ';') {
                addressIndex++;
                name[nameIndex] = fileByte;
                nameIndex++;
                nameHash ^= fileByte;
                nameHash *= FNV_32_PRIME;
            }
            //skip ';'
            addressIndex++;
            //either minus sign or first number
            byte signOrNumber = UNSAFE.getByte(addressIndex);
            //number is in range between -99.9 to 99.9
            //always has 1 fractional digit(E.g -9.9 or -99.9)
            int temperature;
            if (signOrNumber == (byte) '-') {
                //skip minus sign
                addressIndex++;
                //-9.9
                if (UNSAFE.getByte(addressIndex + 3) == (byte) '\n') {
                    temperature = (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                } else {
                    //-99.9
                    temperature = (UNSAFE.getByte(addressIndex) - '0') * 100;
                    addressIndex++;
                    temperature += (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                }
                //skip '.'
                addressIndex++;
                temperature += (UNSAFE.getByte(addressIndex) - '0');
                //go to next line char
                addressIndex++;
                temperature = -temperature;
            } else {
                //go to next index
                addressIndex++;
                //9.9
                if (UNSAFE.getByte(addressIndex + 2) == (byte) '\n') {
                    temperature = (signOrNumber - '0') * 10;
                } else {
                    //99.9
                    temperature = (signOrNumber - '0') * 100;
                    temperature += (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                }
                //skip '.'
                addressIndex++;
                temperature += (UNSAFE.getByte(addressIndex) - '0');
                addressIndex++;
            }
            //go to next line char
            addressIndex++;
            nameHash &= MAP_SIZE;
            map.merge(name, nameIndex, nameHash, temperature);
            //debug
            final byte[] copy = new byte[nameIndex];
            System.arraycopy(name, 0, copy, 0, copy.length);
            System.out.println(new String(copy) + " " + temperature + " " + nameHash);
        }
    }

    private static class LinearProbingMap {

        public void merge(final byte[] name, final int nameIndex, final int nameHash, final int temperature) {

        }
    }

    private static class ResultRow {
        private int min;
        private int max;
        private long sum;
        private int count;

        private ResultRow(int temperature) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
        }
    }

    private record Chunk(long start, long length) {
    }

}
