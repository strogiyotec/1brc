package dev.morling.onebrc;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.reflect.Field;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReferenceArray;
import sun.misc.Unsafe;

public final class CalculateAverage_almas {

    private static final String FILE_PATH = "./measurements.txt";

    private static final Unsafe UNSAFE = initUnsafe();

    private static final int CITY_NAME_MAX_LENGTH = 100;

    private static final int MAP_SIZE = 65536;

    private static final int FNV_32_INIT = 0x811c9dc5;
    private static final int FNV_32_PRIME = 16777619;

    private static final int THREADS_NUMBER = Runtime.getRuntime().availableProcessors() + 1;

    private static final AtomicReferenceArray<LinearProbingMap> RESULTS = new AtomicReferenceArray<>(THREADS_NUMBER);

    /**
     * Moved to a different method to not avoid reordering in case this write was a part of a same method
     */
    private static void updateResult(int index, LinearProbingMap result) {
        RESULTS.set(index, result);
    }

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
        // split and parse chunks
        try (var executor = Executors.newFixedThreadPool(THREADS_NUMBER)) {
            CalculateAverage_almas.computeChunks(channel, THREADS_NUMBER, executor);
        }
        // merge chunks
        LinearProbingMap linearProbingMap = new LinearProbingMap();
        for (int i = 0; i < RESULTS.length(); i++) {
            LinearProbingMap map = RESULTS.get(i);
            linearProbingMap.merge(map);
        }
        System.out.println(report(linearProbingMap));
    }

    private static String report(LinearProbingMap linearProbingMap) {
        ResultRow[] rows = linearProbingMap.toRows();
        Arrays.sort(rows);
        StringBuilder sb = new StringBuilder(MAP_SIZE * 10);
        sb.append('{');
        for (int i = 0; i < rows.length; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            rows[i].print(sb);
        }
        sb.append('}');
        return sb.toString();
    }

    private static void computeChunks(final FileChannel channel, final int threads, final ExecutorService executorService) throws IOException {
        final Instant before = Instant.now();
        final long fileSize = channel.size();
        final long estimatedChunkSize = fileSize / threads;
        final long mappedAddress = channel.map(FileChannel.MapMode.READ_ONLY, 0L, fileSize, Arena.global()).address();
        long chunkStart = 0L;
        long chunkEnd = Math.min(fileSize - 1, estimatedChunkSize);
        int threadId = 0;
        while (chunkStart < fileSize) {
            final long startPosition = chunkStart + chunkEnd;
            final MappedByteBuffer buffer = channel.map(
                FileChannel.MapMode.READ_ONLY,
                startPosition,
                Math.min(
                    fileSize - startPosition, estimatedChunkSize));
            while (buffer.get() != (byte) '\n') {
                chunkEnd++;
            }
            // add a new line
            chunkEnd++;
            final Chunk chunk = new Chunk(
                chunkStart + mappedAddress,
                chunkEnd);
            final int currentThreadId = threadId;
            executorService.submit(() -> CalculateAverage_almas.parseChunk(chunk, currentThreadId));
            chunkStart += chunkEnd;
            chunkEnd = Math.min(estimatedChunkSize, fileSize - chunkStart - 1L/* leave a space for an empty line */);
            threadId++;
        }
        final Instant after = Instant.now();
        System.out.println("Chunks " + Duration.between(before, after).toMillis());
    }

    private static void parseChunk(final Chunk chunk, int currentThreadId) {
        final LinearProbingMap map = new LinearProbingMap();
        // allocate once and then reuse
        final byte[] name = new byte[CITY_NAME_MAX_LENGTH];
        long addressIndex = chunk.start();
        final long chunkLength = chunk.start() + chunk.length();
        while (addressIndex < chunkLength) {
            int nameIndex = 0;
            int nameHash = FNV_32_INIT;
            byte fileByte;
            // compute the hash
            while ((fileByte = UNSAFE.getByte(addressIndex)) != (byte) ';') {
                addressIndex++;
                name[nameIndex] = fileByte;
                nameIndex++;
                nameHash ^= fileByte;
                nameHash *= FNV_32_PRIME;
            }
            // skip ';'
            addressIndex++;
            // either minus sign or first number
            byte signOrNumber = UNSAFE.getByte(addressIndex);
            // number is in range between -99.9 to 99.9
            // always has 1 fractional digit(E.g -9.9 or -99.9)
            int temperature;
            if (signOrNumber == (byte) '-') {
                // skip minus sign
                addressIndex++;
                // -9.9
                if (UNSAFE.getByte(addressIndex + 3) == (byte) '\n') {
                    temperature = (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                } else {
                    // -99.9
                    temperature = (UNSAFE.getByte(addressIndex) - '0') * 100;
                    addressIndex++;
                    temperature += (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                }
                // skip '.'
                addressIndex++;
                temperature += (UNSAFE.getByte(addressIndex) - '0');
                // go to next line char
                addressIndex++;
                temperature = -temperature;
            } else {
                // go to next index
                addressIndex++;
                // 9.9
                if (UNSAFE.getByte(addressIndex + 2) == (byte) '\n') {
                    temperature = (signOrNumber - '0') * 10;
                } else {
                    // 99.9
                    temperature = (signOrNumber - '0') * 100;
                    temperature += (UNSAFE.getByte(addressIndex) - '0') * 10;
                    addressIndex++;
                }
                // skip '.'
                addressIndex++;
                temperature += (UNSAFE.getByte(addressIndex) - '0');
                addressIndex++;
            }
            // go to next line char
            addressIndex++;
            // shrink the hash size based on the map size limit
            nameHash &= (MAP_SIZE - 1);

            map.mergeRow(name, nameIndex, nameHash, temperature);
        }
        updateResult(currentThreadId, map);
    }

    private static class LinearProbingMap {

        private final ResultRow[] array = new ResultRow[CalculateAverage_almas.MAP_SIZE];

        public void mergeRow(final byte[] name, final int nameLength, final int hash, final int temperature) {
            int slotIndex = hash;
            ResultRow existingRow;
            while (true) {
                existingRow = this.array[slotIndex];
                if (existingRow != null && existingRow.hasDifferentName(hash, name, nameLength)) {
                    slotIndex++;
                } else {
                    break;
                }
                slotIndex++;
            }
            if (existingRow != null) {
                existingRow.merge(temperature);
            } else {
                // need to copy it out because same name array will be reused for all remaining rows in this chunk
                byte[] copiedName = new byte[nameLength];
                System.arraycopy(name, 0, copiedName, 0, nameLength);

                ResultRow resultRow = new ResultRow(temperature, copiedName, hash);
                this.array[slotIndex] = resultRow;
            }
        }

        public void merge(LinearProbingMap otherMap) {
            for (ResultRow otherRow : otherMap.array) {
                if (otherRow == null) {
                    continue;
                }
                int index = otherRow.nameHashCode;
                while (true) {
                    ResultRow currentRow = this.array[index];
                    // entry exists in other map but not in current map
                    if (currentRow == null) {
                        this.array[index] = otherRow;
                        break;
                    } else if (!otherRow.hasDifferentName(currentRow.nameHashCode, currentRow.nameBytes, currentRow.nameBytes.length)) {
                        currentRow.merge(otherRow);
                        break;
                    } else {
                        index = (index + 1) & (MAP_SIZE - 1);
                    }
                }
            }
        }

        public ResultRow[] toRows() {
            ResultRow[] resultRows = new ResultRow[this.array.length];
            int index = 0;
            for (ResultRow resultRow : this.array) {
                if (resultRow != null) {
                    resultRow.initName();
                    resultRows[index] = resultRow;
                    index++;
                }
            }
            ResultRow[] withoutNull = new ResultRow[index];
            System.arraycopy(resultRows, 0, withoutNull, 0, withoutNull.length);
            return withoutNull;
        }

    }

    private static class ResultRow implements Comparable<ResultRow> {
        private int min;
        private int max;
        private long sum;
        private int count;

        /**
         * Bytes representation of the name,
         * Used to efficiently compare strings using unsafe
         * instead of char by char
         */
        private byte[] nameBytes;

        private int nameHashCode;

        private String name;

        private ResultRow(int temperature, byte[] nameBytes, int hashCode) {
            this.min = temperature;
            this.max = temperature;
            this.sum = temperature;
            this.count = 1;
            this.nameBytes = nameBytes;
            this.nameHashCode = hashCode;
        }

        @Override
        public int compareTo(ResultRow o) {
            return this.name.compareTo(o.name);
        }

        private double round(double value) {
            return Math.round(value) / 10.0;
        }

        public void print(StringBuilder sb) {
            sb.append(this.name);
            sb.append("=");
            sb.append(round(min));
            sb.append("/");
            sb.append(round((double) sum / count));
            sb.append("/");
            sb.append(round(max));
        }

        /**
         * At this stage the name array has some garbage in the tail
         * as this same array is being reused for every new row in the file
         * That's why we pass second argument `nameLength` that indicates a real
         * length of the name in this array
         */
        public boolean hasDifferentName(int hash, byte[] name, int nameLength) {
            boolean diffName = this.nameHashCode != hash || nameLength != this.nameBytes.length || !this.fastEquals(name, this.nameBytes, nameLength);
            return diffName;
        }

        private boolean fastEquals(final byte[] first, final byte[] second, final int length) {
            final int baseOffset = Unsafe.ARRAY_BYTE_BASE_OFFSET;
            int index = 0;
            for (; index < length - Long.BYTES; index += 8) {
                if (UNSAFE.getLong(first, index + baseOffset) != UNSAFE.getLong(second, index + baseOffset)) {
                    return false;
                }
            }
            if (index == length) {
                return true;
            }
            for (; index < length - Integer.BYTES; index += 4) {
                if (UNSAFE.getInt(first, index + baseOffset) != UNSAFE.getInt(second, index + baseOffset)) {
                    return false;
                }
            }
            if (index == length) {
                return true;
            }
            for (; index < length; index++) {
                if (UNSAFE.getByte(first, index + baseOffset) != UNSAFE.getByte(second, index + baseOffset)) {
                    return false;
                }
            }
            return true;
        }

        public void merge(ResultRow other) {
            this.min = Math.min(this.min, other.min);
            this.max = Math.max(this.max, other.max);
            this.sum += other.sum;
            this.count += other.count;
        }

        public void merge(int temperature) {
            this.min = Math.min(this.min, temperature);
            this.max = Math.max(this.max, temperature);
            this.sum += temperature;
            this.count++;
        }

        public void initName() {
            this.name = new String(this.nameBytes, StandardCharsets.UTF_8);
        }
    }

    private record Chunk(long start, long length) {
    }

}
