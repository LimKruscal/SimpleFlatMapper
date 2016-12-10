package org.simpleflatmapper.util;

import java.io.IOException;
import java.io.Reader;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

public class ParallelReader extends Reader {
    private static final int DEFAULT_MAX_READ = 8192;
    private static final int DEFAULT_BUFFER_SIZE = 1024 * 32;
    public static final int PADDING = 64;
    public static final int PADDING_THRESHOLD = 1024;

    private final DataProducer dataProducer;
    private final char[] readBuffer;

    private final int bufferMask;
    private final long capacity;

    private final AtomicLong tail;
    private final AtomicLong head;

    private long tailCache;

    public ParallelReader(Reader reader, Executor executorService) {
        this(reader, executorService, DEFAULT_BUFFER_SIZE);
    }

    public ParallelReader(Reader reader, Executor executorService, int bufferSize) {
        this(reader, executorService, bufferSize, DEFAULT_MAX_READ);
    }
    public ParallelReader(Reader reader, Executor executorService, int bufferSize, int maxRead) {
        bufferSize = toPowerOfTwo(bufferSize);

        readBuffer = new char[bufferSize];

        tail = new AtomicLong();
        head = new AtomicLong();

        dataProducer = new DataProducer(reader, readBuffer, bufferSize <= PADDING_THRESHOLD ? 0 : PADDING, maxRead, tail, head);

        executorService.execute(dataProducer);

        bufferMask = readBuffer.length - 1;
        capacity = readBuffer.length;

    }

    private static int toPowerOfTwo(int bufferSize) {
        return 1 << 32 - Integer.numberOfLeadingZeros(bufferSize - 1);
    }

    @Override
    public int read(char[] cbuf, int off, int len) throws IOException {
        final long currentHead = head.get();
        do {
            if (currentHead < tailCache) {
                int l = read(cbuf, off, len, currentHead, tailCache);

                head.lazySet(currentHead + l);
                return l;
            }

            tailCache = tail.get();
            if (currentHead >= tailCache) {
                if (!dataProducer.run) {
                    if (dataProducer.exception != null) {
                        throw dataProducer.exception;
                    }
                    tailCache = tail.get();
                    if (currentHead >= tailCache) {
                        return -1;
                    }
                }
                waitingStrategy();
            }
        } while(true);
    }

    @Override
    public int read() throws IOException {

        final long currentHead = head.get();
        do {
            if (currentHead < tailCache) {

                int headIndex = (int) (currentHead & bufferMask);

                char c = readBuffer[headIndex];

                head.lazySet(currentHead + 1);

                return c;
            }

            tailCache = tail.get();
            if (currentHead >= tailCache) {
                if (!dataProducer.run) {
                    if (dataProducer.exception != null) {
                        throw dataProducer.exception;
                    }
                    tailCache = tail.get();
                    if (currentHead >= tailCache) {
                        return -1;
                    }
                }
                waitingStrategy();
            }
        } while(true);
    }

    private static void waitingStrategy() {
        Thread.yield();
    }

    private int read(char[] cbuf, int off, int len, long currentHead, long currentTail) {

        int headIndex = (int) (currentHead & bufferMask);
        int usedLength = (int) (currentTail - currentHead);

        int block1Length = Math.min(len, Math.min(usedLength, (int) (capacity - headIndex)));
        int block2Length =  Math.min(len, usedLength) - block1Length;

        System.arraycopy(readBuffer, headIndex, cbuf, off, block1Length);
        System.arraycopy(readBuffer, 0, cbuf, off+ block1Length, block2Length);

        return block1Length + block2Length;
    }

    @Override
    public void close() throws IOException {
        dataProducer.stop();
    }

    private static class DataProducer implements Runnable {
        private volatile boolean run = true;
        private volatile IOException exception;

        private final Reader reader;
        private final char[] writeBuffer;
        private final long readWriteZonePadding;
        private final int maxRead;

        private final int bufferMask;
        private final int capacity;

        private final AtomicLong tail;
        private final AtomicLong head;

        private long headCache;

        private DataProducer(Reader reader, char[] writeBuffer, long readWriteZonePadding, int maxRead, AtomicLong tail, AtomicLong head) {
            this.reader = reader;
            this.writeBuffer = writeBuffer;
            this.readWriteZonePadding = readWriteZonePadding;
            this.maxRead = maxRead;

            bufferMask = writeBuffer.length - 1;
            capacity = writeBuffer.length;
            this.tail = tail;

            this.head = head;
        }

        @Override
        public void run() {
            long currentTail = tail.get();
            while(run) {

                final long wrapPoint = currentTail - writeBuffer.length;

                if (headCache - readWriteZonePadding <= wrapPoint) {
                    headCache = head.get();
                    if (headCache - readWriteZonePadding <= wrapPoint) {
                        waitingStrategy();
                        continue;
                    }
                }

                try {
                    int r =  read(currentTail, headCache);
                    if (r == -1) {
                        run = false;
                    } else {
                        currentTail += r;
                        tail.lazySet(currentTail);
                    }
                } catch (IOException e) {
                    exception = e;
                    run = false;
                }
            }
        }

        private int read(long currentTail, long currentHead) throws IOException {
            long used = currentTail - currentHead;

            long length = Math.min(capacity - used, maxRead);

            int tailIndex = (int) (currentTail & bufferMask);

            int endBlock1 = (int) Math.min(tailIndex + length,  capacity);

            int block1Length = endBlock1 - tailIndex;

            return reader.read(writeBuffer, tailIndex, block1Length);
        }

        public void stop() {
            run = false;
        }
    }
}
