/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.translog;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.procedures.LongProcedure;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.opensearch.Assertions;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.Channels;
import org.opensearch.common.io.DiskIoBufferPool;
import org.opensearch.common.io.stream.ReleasableBytesStreamOutput;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.concurrent.ReleasableLock;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;
// 根据commit：7f8e1454的info 会追踪seqNo not been fsynced.Once they are fsynced, TranslogWriter notifies LocalCheckpointTracker of this.
public class TranslogWriter extends BaseTranslogReader implements Closeable {

    private final ShardId shardId; // 有shardId，说明是shard级别的
    private final FileChannel checkpointChannel;
    private final Path checkpointPath;
    private final BigArrays bigArrays;
    // the last checkpoint that was written when the translog was last synced
    private volatile Checkpoint lastSyncedCheckpoint;
    /* the number of translog operations written to this file */
    private volatile int operationCounter;
    /* if we hit an exception that we can't recover from we assign it to this var and ship it with every AlreadyClosedException we throw */
    private final TragicExceptionHolder tragedy;
    /* the total offset of this file including the bytes written to the file as well as into the buffer */
    private volatile long totalOffset;

    private volatile long minSeqNo;
    private volatile long maxSeqNo;

    private final LongSupplier globalCheckpointSupplier;
    private final LongSupplier minTranslogGenerationSupplier;

    // callback that's called whenever an operation with a given sequence number is successfully persisted.
    private final LongConsumer persistedSequenceNumberConsumer;

    protected final AtomicBoolean closed = new AtomicBoolean(false);
    // lock order try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final ReleasableLock writeLock = new ReleasableLock(new ReentrantLock());
    // lock order synchronized(syncLock) -> try(Releasable lock = writeLock.acquire()) -> synchronized(this)
    private final Object syncLock = new Object();

    private LongArrayList nonFsyncedSequenceNumbers = new LongArrayList(64);
    private final int forceWriteThreshold;
    private volatile long bufferedBytes;
    private ReleasableBytesStreamOutput buffer;

    private final Map<Long, Tuple<BytesReference, Exception>> seenSequenceNumbers;

    private TranslogWriter(
        final ShardId shardId,
        final Checkpoint initialCheckpoint,
        final FileChannel channel,
        final FileChannel checkpointChannel,
        final Path path,
        final Path checkpointPath,
        final ByteSizeValue bufferSize,
        final LongSupplier globalCheckpointSupplier,
        LongSupplier minTranslogGenerationSupplier,
        TranslogHeader header,
        final TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays
    ) throws IOException {
        super(initialCheckpoint.generation, channel, path, header);
        assert initialCheckpoint.offset == channel.position() : "initial checkpoint offset ["
            + initialCheckpoint.offset
            + "] is different than current channel position ["
            + channel.position()
            + "]";
        this.forceWriteThreshold = Math.toIntExact(bufferSize.getBytes());
        this.shardId = shardId;
        this.checkpointChannel = checkpointChannel;
        this.checkpointPath = checkpointPath;
        this.minTranslogGenerationSupplier = minTranslogGenerationSupplier;
        this.lastSyncedCheckpoint = initialCheckpoint;
        this.totalOffset = initialCheckpoint.offset;
        assert initialCheckpoint.minSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.minSeqNo;
        this.minSeqNo = initialCheckpoint.minSeqNo;
        assert initialCheckpoint.maxSeqNo == SequenceNumbers.NO_OPS_PERFORMED : initialCheckpoint.maxSeqNo;
        this.maxSeqNo = initialCheckpoint.maxSeqNo;
        assert initialCheckpoint.trimmedAboveSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO : initialCheckpoint.trimmedAboveSeqNo;
        this.globalCheckpointSupplier = globalCheckpointSupplier;
        this.persistedSequenceNumberConsumer = persistedSequenceNumberConsumer;
        this.bigArrays = bigArrays;
        this.seenSequenceNumbers = Assertions.ENABLED ? new HashMap<>() : null;
        this.tragedy = tragedy;
    }

    public static TranslogWriter create(
        ShardId shardId,
        String translogUUID,
        long fileGeneration,
        Path file, // file 是 .tlog文件
        ChannelFactory channelFactory,
        ByteSizeValue bufferSize, // 根据调用关系，可以知道：默认org.opensearch.index.translog.TranslogConfig.DEFAULT_BUFFER_SIZE：1Mb，除却EmptyTranslog是10bytes
        final long initialMinTranslogGen,
        long initialGlobalCheckpoint,
        final LongSupplier globalCheckpointSupplier,
        final LongSupplier minTranslogGenerationSupplier,//用于部分判断是否要建立新的checkpoint
        final long primaryTerm,
        TragicExceptionHolder tragedy,
        final LongConsumer persistedSequenceNumberConsumer,
        final BigArrays bigArrays
    ) throws IOException {
        final Path checkpointFile = file.getParent().resolve(Translog.CHECKPOINT_FILE_NAME);//老规矩，还是找到translog.cpk

        final FileChannel channel = channelFactory.open(file);// 打开tlog文件
        FileChannel checkpointChannel = null;
        try {
            checkpointChannel = channelFactory.open(checkpointFile, StandardOpenOption.WRITE); // 打开checkpoint文件的channel
            final TranslogHeader header = new TranslogHeader(translogUUID, primaryTerm);
            header.write(channel);//给tlog写入header
            final Checkpoint checkpoint = Checkpoint.emptyTranslogCheckpoint(
                header.sizeInBytes(),
                fileGeneration,
                initialGlobalCheckpoint,
                initialMinTranslogGen
            );
            writeCheckpoint(checkpointChannel, checkpointFile, checkpoint); // 将checkpoint的信息，加上checkpointFile的文件名写入到 checkpointChannel
            final LongSupplier writerGlobalCheckpointSupplier;
            if (Assertions.ENABLED) {
                writerGlobalCheckpointSupplier = () -> {
                    long gcp = globalCheckpointSupplier.getAsLong();
                    assert gcp >= initialGlobalCheckpoint : "global checkpoint ["
                        + gcp
                        + "] lower than initial gcp ["
                        + initialGlobalCheckpoint
                        + "]";
                    return gcp;
                };
            } else {
                writerGlobalCheckpointSupplier = globalCheckpointSupplier;
            }
            return new TranslogWriter(
                shardId,
                checkpoint, // 新的checkpoint
                channel, // tlog的channel
                checkpointChannel, // checkpoint的cpk的channel
                file,// tlog文件的path
                checkpointFile, // cpk checkpoint文件的path
                bufferSize,
                writerGlobalCheckpointSupplier, // globalCheckpoint
                minTranslogGenerationSupplier, //  时刻可以获得Translog的最新的最小的文件引用
                header,
                tragedy,
                persistedSequenceNumberConsumer,
                bigArrays
            );
        } catch (Exception exception) {
            // if we fail to bake the file-generation into the checkpoint we stick with the file and once we recover and that
            // file exists we remove it. We only apply this logic to the checkpoint.generation+1 any other file with a higher generation
            // is an error condition
            IOUtils.closeWhileHandlingException(channel, checkpointChannel);
            throw exception;
        }
    }

    private synchronized void closeWithTragicEvent(final Exception ex) {
        tragedy.setTragicException(ex);
        try {
            close();
        } catch (final IOException | RuntimeException e) {
            ex.addSuppressed(e);
        }
    }

    /**
     * Add the given bytes to the translog with the specified sequence number; returns the location the bytes were written to.
     *
     * @param data  the bytes to write
     * @param seqNo the sequence number associated with the operation
     * @return the location the bytes were written to
     * @throws IOException if writing to the translog resulted in an I/O exception
     */
    public Translog.Location add(final BytesReference data, final long seqNo) throws IOException {
        long bufferedBytesBeforeAdd = this.bufferedBytes; // 先获取写入前的bufferBytes有多少
        if (bufferedBytesBeforeAdd >= forceWriteThreshold) { // 每次add的时候，先判断缓存里的数据大小，需不需要先刷盘,避免#63299的问题
            writeBufferedOps(Long.MAX_VALUE, bufferedBytesBeforeAdd >= forceWriteThreshold * 4);
        } // todo: 为什么要 大于 4倍
        // 1.
        final Translog.Location location;
        synchronized (this) { //所以add操作一定是互斥的，但是writeBufferedOps和add却不是互相阻塞的，又一个线程在writeBufferedOps，其他还是能add的，具体是受益于那个lock
            ensureOpen();
            if (buffer == null) {//buffer为空的情况是 writeBufferedOps后置空的
                buffer = new ReleasableBytesStreamOutput(bigArrays);//获取一个和pagesize一样16KB大小的缓存
            }
            assert bufferedBytes == buffer.size();//这两个值应该是相等的
            final long offset = totalOffset;// totalOffset是迄今为止，translogWriter写入的bytes
            totalOffset += data.length();
            data.writeTo(buffer);

            assert minSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;//要么minSeqNo不是没有操作的，要么操作的count==0
            assert maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED || operationCounter == 0;//
            // 获取此writer里面的所有操作中的已知的最小和最大的操作序列。是个range。 #22822：为了重放某个特定seqNo之后的所有operation
            minSeqNo = SequenceNumbers.min(minSeqNo, seqNo);
            maxSeqNo = SequenceNumbers.max(maxSeqNo, seqNo);
//此SeqNo的operation只是写入到TranslogWriter的缓存buffer里，还没有fsync到持久化  #43205：now keeps track of the sequence numbers that have not been fsynced yet. Once they are fsynced, TranslogWriter notifies LocalCheckpointTracker of this.
            nonFsyncedSequenceNumbers.add(seqNo);

            operationCounter++;

            assert assertNoSeqNumberConflict(seqNo, data);

            location = new Translog.Location(generation, offset, data.length());
            bufferedBytes = buffer.size();
        }

        return location;
    }
    // 确保此seqNo在此writer里不会有任何冲突，并且记录下operation的data在内存里
    private synchronized boolean assertNoSeqNumberConflict(long seqNo, BytesReference data) throws IOException {
        if (seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO) {
            // nothing to do // 如果包含，说明此SeqNo已经在TranslogWriter里处理过
        } else if (seenSequenceNumbers.containsKey(seqNo)) {
            final Tuple<BytesReference, Exception> previous = seenSequenceNumbers.get(seqNo);
            if (previous.v1().equals(data) == false) {
                Translog.Operation newOp = Translog.readOperation(new BufferedChecksumStreamInput(data.streamInput(), "assertion"));
                Translog.Operation prvOp = Translog.readOperation(
                    new BufferedChecksumStreamInput(previous.v1().streamInput(), "assertion")
                );
                // TODO: We haven't had timestamp for Index operations in Lucene yet, we need to loosen this check without timestamp.
                final boolean sameOp;
                if (newOp instanceof Translog.Index && prvOp instanceof Translog.Index) {
                    final Translog.Index o1 = (Translog.Index) prvOp;
                    final Translog.Index o2 = (Translog.Index) newOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.type(), o2.type())
                        && Objects.equals(o1.source(), o2.source())
                        && Objects.equals(o1.routing(), o2.routing())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else if (newOp instanceof Translog.Delete && prvOp instanceof Translog.Delete) {
                    final Translog.Delete o1 = (Translog.Delete) newOp;
                    final Translog.Delete o2 = (Translog.Delete) prvOp;
                    sameOp = Objects.equals(o1.id(), o2.id())
                        && Objects.equals(o1.type(), o2.type())
                        && o1.primaryTerm() == o2.primaryTerm()
                        && o1.seqNo() == o2.seqNo()
                        && o1.version() == o2.version();
                } else {
                    sameOp = false;
                }
                if (sameOp == false) {
                    throw new AssertionError(
                        "seqNo ["
                            + seqNo
                            + "] was processed twice in generation ["
                            + generation
                            + "], with different data. "
                            + "prvOp ["
                            + prvOp
                            + "], newOp ["
                            + newOp
                            + "]",
                        previous.v2()
                    );
                }
            }
        } else {
            seenSequenceNumbers.put(
                seqNo,
                new Tuple<>(new BytesArray(data.toBytesRef(), true), new RuntimeException("stack capture previous op"))
            );
        }
        return true;
    }
    //assert当前的writer没有aboveSeqNo以上的operation;用每个大于aboveSeq的SeqNo获取它的operation详情，然后做比较
    synchronized boolean assertNoSeqAbove(long belowTerm, long aboveSeqNo) {
        seenSequenceNumbers.entrySet().stream().filter(e -> e.getKey().longValue() > aboveSeqNo).forEach(e -> {
            final Translog.Operation op;
            try {
                op = Translog.readOperation(new BufferedChecksumStreamInput(e.getValue().v1().streamInput(), "assertion"));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
            long seqNo = op.seqNo();
            long primaryTerm = op.primaryTerm();
            if (primaryTerm < belowTerm) {
                throw new AssertionError(
                    "current should not have any operations with seq#:primaryTerm ["
                        + seqNo
                        + ":"
                        + primaryTerm
                        + "] > "
                        + aboveSeqNo
                        + ":"
                        + belowTerm
                );
            }
        });
        return true;
    }

    /**
     * write all buffered ops to disk and fsync file.
     *
     * Note: any exception during the sync process will be interpreted as a tragic exception and the writer will be closed before
     * raising the exception.
     */
    public void sync() throws IOException {
        syncUpTo(Long.MAX_VALUE);
    }

    /**
     * Returns <code>true</code> if there are buffered operations that have not been flushed and fsynced to disk or if the latest global
     * checkpoint has not yet been fsynced
     */// 1 和 2都能理解，todo：但是三是什么情况？
    public boolean syncNeeded() {
        return totalOffset != lastSyncedCheckpoint.offset
            || globalCheckpointSupplier.getAsLong() != lastSyncedCheckpoint.globalCheckpoint
            || minTranslogGenerationSupplier.getAsLong() != lastSyncedCheckpoint.minTranslogGeneration;
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    synchronized Checkpoint getCheckpoint() {
        return new Checkpoint(
            totalOffset,
            operationCounter,
            generation,
            minSeqNo,
            maxSeqNo,
            globalCheckpointSupplier.getAsLong(),
            minTranslogGenerationSupplier.getAsLong(),
            SequenceNumbers.UNASSIGNED_SEQ_NO
        );
    }

    @Override
    public long sizeInBytes() {
        return totalOffset;
    }

    /**
     * Closes this writer and transfers its underlying file channel to a new immutable {@link TranslogReader}
     * @return a new {@link TranslogReader}
     * @throws IOException if any of the file operations resulted in an I/O exception
     */
    public TranslogReader closeIntoReader() throws IOException {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        //
        // Note: While this is not strictly needed as this method is called while blocking all ops on the translog,
        // we do this to for correctness and preventing future issues.
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    try {
                        sync(); // sync before we close..
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer == null;
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    if (closed.compareAndSet(false, true)) {
                        try {
                            checkpointChannel.close();
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                        return new TranslogReader(getLastSyncedCheckpoint(), channel, path, header);
                    } else {
                        throw new AlreadyClosedException(
                            "translog [" + getGeneration() + "] is already closed (path [" + path + "]",
                            tragedy.get()
                        );
                    }
                }
            }
        }
    }

    @Override
    public TranslogSnapshot newSnapshot() {
        // make sure to acquire the sync lock first, to prevent dead locks with threads calling
        // syncUpTo() , where the sync lock is acquired first, following by the synchronize(this)
        // After the sync lock we acquire the write lock to avoid deadlocks with threads writing where
        // the write lock is acquired first followed by synchronize(this).
        synchronized (syncLock) {
            try (ReleasableLock toClose = writeLock.acquire()) {
                synchronized (this) {
                    ensureOpen();
                    try {
                        sync();
                    } catch (IOException e) {
                        throw new TranslogException(shardId, "exception while syncing before creating a snapshot", e);
                    }
                    // If we reached this point, all of the buffered ops should have been flushed successfully.
                    assert buffer == null;
                    assert checkChannelPositionWhileHandlingException(totalOffset);
                    assert totalOffset == lastSyncedCheckpoint.offset;
                    return super.newSnapshot();
                }
            }
        }
    }

    private long getWrittenOffset() throws IOException {
        return channel.position();
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     *///如果上一次synced的TranslogCheckpoint比要求的小，且needed； 只有一个线程能进入方法；完成后更新lastSyncedCheckpoint
    final boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
            synchronized (syncLock) { // only one sync/checkpoint should happen concurrently but we wait
                if (lastSyncedCheckpoint.offset < offset && syncNeeded()) {
                    // double checked locking - we don't want to fsync unless we have to and now that we have
                    // the lock we should check again since if this code is busy we might have fsynced enough already
                    final Checkpoint checkpointToSync;
                    final LongArrayList flushedSequenceNumbers;
                    final ReleasableBytesReference toWrite;
                    try (ReleasableLock toClose = writeLock.acquire()) {
                        synchronized (this) {
                            ensureOpen();
                            checkpointToSync = getCheckpoint();//生成想要生成的新的TranslogCheckpoint；护环seqnums，因为sync的存在，不会并发继续向nonFlushed里面写
                            toWrite = pollOpsToWrite();
                            flushedSequenceNumbers = nonFsyncedSequenceNumbers;
                            nonFsyncedSequenceNumbers = new LongArrayList(64);
                        }

                        try {
                            // Write ops will release operations. //写入文件，实际是操作系统的文件缓存
                            writeAndReleaseOps(toWrite);
                        } catch (final Exception ex) {
                            closeWithTragicEvent(ex);
                            throw ex;
                        }
                    }
                    // now do the actual fsync outside of the synchronized block such that
                    // we can continue writing to the buffer etc.等于上面上锁，是为了腾出buffer，好让其他线程继续add,然后强制tlog的数据落盘，之后在写checkpoint文件，写新的checkpoint
                    try {// 强制刷操作系统缓存到磁盘
                        channel.force(false);
                        writeCheckpoint(checkpointChannel, checkpointPath, checkpointToSync);
                    } catch (final Exception ex) {
                        closeWithTragicEvent(ex);
                        throw ex;
                    }
                    flushedSequenceNumbers.forEach((LongProcedure) persistedSequenceNumberConsumer::accept);
                    assert lastSyncedCheckpoint.offset <= checkpointToSync.offset : "illegal state: "
                        + lastSyncedCheckpoint.offset
                        + " <= "
                        + checkpointToSync.offset;
                    lastSyncedCheckpoint = checkpointToSync; // write protected by syncLock
                    return true;
                }
            }
        }
        return false;
    }
    // 将TranslogWriter的缓冲区的数据写入到磁盘，并且依据#63374，简单来说：之前每次向Writer添加一个operation都是同步的，并且判断是否到达8kb，然后发起系统调用写入。这样在发起系统调用的时候，会阻塞其他的add。所以这个pr是把add放到列表里，列表在达到1mb或者主动触发的时候被sync
    private void writeBufferedOps(long offset, boolean blockOnExistingWriter) throws IOException {
        try (ReleasableLock locked = blockOnExistingWriter ? writeLock.acquire() : writeLock.tryAcquire()) {
            try {// 意思是获取到了锁，且期望小于offset，大于getWrittenOffset的都要被写入到channel，getWrittenOffset以下的是已经写入到tlog的channel
                if (locked != null && offset > getWrittenOffset()) {
                    writeAndReleaseOps(pollOpsToWrite()); // 先获取到哪些需要写入文件系统的，然后写入，写入之后，清空那部分的内存
                }
            } catch (Exception e) {
                closeWithTragicEvent(e);
                throw e;
            }
        }
    }

    private synchronized ReleasableBytesReference pollOpsToWrite() {
        ensureOpen();
        if (this.buffer != null) {
            ReleasableBytesStreamOutput toWrite = this.buffer;
            this.buffer = null;
            this.bufferedBytes = 0;
            return new ReleasableBytesReference(toWrite.bytes(), toWrite);
        } else {
            return ReleasableBytesReference.wrap(BytesArray.EMPTY);
        }
    }

    private void writeAndReleaseOps(ReleasableBytesReference toWrite) throws IOException {
        try (ReleasableBytesReference toClose = toWrite) {
            assert writeLock.isHeldByCurrentThread();
            ByteBuffer ioBuffer = DiskIoBufferPool.getIoBuffer();//此刻应该是write，fresh线程，所以应该是DirectByteBuffer
            //DirectByteBuffer, 底层数据是维护在操作系统内存中，不在jvm，只维护引用地址指向数据。通常从外部设备读取时，jvm从这个外部设备的数据读取到一个内存块，再从这个内存块读取。但是使用DirectByteBuffer 不需要从外部设备读取到内存，可以直接读取。
            BytesRefIterator iterator = toWrite.iterator(); // 这样的优点是：1. 零拷贝 2. 避免gc压力
            BytesRef current;
            while ((current = iterator.next()) != null) {
                int currentBytesConsumed = 0;
                while (currentBytesConsumed != current.length) {
                    int nBytesToWrite = Math.min(current.length - currentBytesConsumed, ioBuffer.remaining());
                    ioBuffer.put(current.bytes, current.offset + currentBytesConsumed, nBytesToWrite);
                    currentBytesConsumed += nBytesToWrite;
                    if (ioBuffer.hasRemaining() == false) {
                        ioBuffer.flip();
                        writeToFile(ioBuffer); // 写到FileChannel里面，不过只是写入了，还没刷新；刷新要对FileChannel调用force操作
                        ioBuffer.clear();
                    }
                }
            }
            ioBuffer.flip();
            writeToFile(ioBuffer);// 将direct 内存写到文件，实际是操作系统文件缓存
        }
    }

    @SuppressForbidden(reason = "Channel#write")
    private void writeToFile(ByteBuffer ioBuffer) throws IOException {
        while (ioBuffer.remaining() > 0) {
            channel.write(ioBuffer);// 所以是写到操作系统缓存，即PageCache
        }
    }
    //父类的注释:reads bytes at position into the given buffer, filling it.//remaining > writtenOffset - position
    @Override//要从writer中从position读出bytes，写入到剩余的targetBuffer中。为什么要writerBufferedOps，因为读出的结果需要被承诺是不可变的。所以读出来之前，确保要进行一定的持久化
    protected void readBytes(ByteBuffer targetBuffer, long position) throws IOException {
        try {
            if (position + targetBuffer.remaining() > getWrittenOffset()) {
                // we only flush here if it's really really needed - try to minimize the impact of the read operation
                // in some cases ie. a tragic event we might still be able to read the relevant value
                // which is not really important in production but some test can make most strict assumptions
                // if we don't fail in this call unless absolutely necessary.
                writeBufferedOps(position + targetBuffer.remaining(), true);
            }
        } catch (final Exception ex) {
            closeWithTragicEvent(ex);
            throw ex;
        }
        // we don't have to have a lock here because we only write ahead to the file, so all writes has been complete
        // for the requested location.//从tlog的channel里的positon位置，读取数据填满targetBuffer
        Channels.readFromFileChannelWithEofException(channel, position, targetBuffer);
    }
//写入translog的提交文件
    private static void writeCheckpoint(final FileChannel fileChannel, final Path checkpointFile, final Checkpoint checkpoint)
        throws IOException {
        Checkpoint.write(fileChannel, checkpointFile, checkpoint);
    }

    /**
     * The last synced checkpoint for this translog.
     *
     * @return the last synced checkpoint
     */
    Checkpoint getLastSyncedCheckpoint() {
        return lastSyncedCheckpoint;
    }

    protected final void ensureOpen() {
        if (isClosed()) {
            throw new AlreadyClosedException("translog [" + getGeneration() + "] is already closed", tragedy.get());
        }
    }

    private boolean checkChannelPositionWhileHandlingException(long expectedOffset) {
        try {
            return expectedOffset == channel.position();
        } catch (IOException e) {
            return true;
        }
    }

    @Override
    public final void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            synchronized (this) {
                Releasables.closeWhileHandlingException(buffer);
                buffer = null;
                bufferedBytes = 0;
            }
            IOUtils.close(checkpointChannel, channel);
        }
    }

    protected final boolean isClosed() {
        return closed.get();
    }
}
