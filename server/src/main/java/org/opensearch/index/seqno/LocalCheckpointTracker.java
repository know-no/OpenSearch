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

package org.opensearch.index.seqno;

import com.carrotsearch.hppc.LongObjectHashMap;
import org.opensearch.common.SuppressForbidden;

import java.util.concurrent.atomic.AtomicLong;

/**
 * This class generates sequences numbers and keeps track of the so-called "local checkpoint" which is the highest number for which all
 * previous sequence numbers have been processed (inclusive).
 */
public class LocalCheckpointTracker {

    /**
     * We keep a bit for each sequence number that is still pending. To optimize allocation, we do so in multiple sets allocating them on
     * demand and cleaning up while completed. This constant controls the size of the sets.
     *///对在pending的seqNo，保存它的一个bit？  这个bit会 对应一个 set
    static final short BIT_SET_SIZE = 1024;

    /**
     * A collection of bit sets representing processed sequence numbers. Each sequence number is mapped to a bit set by dividing by the
     * bit set size.
     */
    final LongObjectHashMap<CountedBitSet> processedSeqNo = new LongObjectHashMap<>();

    /**
     * A collection of bit sets representing durably persisted sequence numbers. Each sequence number is mapped to a bit set by dividing by
     * the bit set size.
     */ // 代表着被持久化的seqNo集合。 为什么用Map以及一个CountedBitSet来表示 // Map的key相当于切分区间，对应的value是一个CountedBitSet
    final LongObjectHashMap<CountedBitSet> persistedSeqNo = new LongObjectHashMap<>();//每个set里面的每个元素代表着一个seqNum在这个区间里的余值

    /**
     * The current local checkpoint, i.e., all sequence numbers no more than this number have been processed.
     */ //见 org.opensearch.index.engine.InternalEngine.index 1096行，只要写入translog就算是处理过了
    final AtomicLong processedCheckpoint = new AtomicLong();

    /**
     * The current persisted local checkpoint, i.e., all sequence numbers no more than this number have been durably persisted.
     */
    final AtomicLong persistedCheckpoint = new AtomicLong(); // todo 什么时候算是 persisted了呢？是已经在吗

    /**
     * The next available sequence number.
     */
    final AtomicLong nextSeqNo = new AtomicLong();

    /**
     * Initialize the local checkpoint service. The {@code maxSeqNo} should be set to the last sequence number assigned, or
     * {@link SequenceNumbers#NO_OPS_PERFORMED} and {@code localCheckpoint} should be set to the last known local checkpoint,
     * or {@link SequenceNumbers#NO_OPS_PERFORMED}.
     *
     * @param maxSeqNo        the last sequence number assigned, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     * @param localCheckpoint the last known local checkpoint, or {@link SequenceNumbers#NO_OPS_PERFORMED}
     *///已有index的也是从segmentInfo里读取到的
    public LocalCheckpointTracker(final long maxSeqNo, final long localCheckpoint) {
        if (localCheckpoint < 0 && localCheckpoint != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "local checkpoint must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] " + "but was [" + localCheckpoint + "]"
            );
        }
        if (maxSeqNo < 0 && maxSeqNo != SequenceNumbers.NO_OPS_PERFORMED) {
            throw new IllegalArgumentException(
                "max seq. no. must be non-negative or [" + SequenceNumbers.NO_OPS_PERFORMED + "] but was [" + maxSeqNo + "]"
            );
        }
        nextSeqNo.set(maxSeqNo + 1);
        processedCheckpoint.set(localCheckpoint);
        persistedCheckpoint.set(localCheckpoint);
    }

    /**
     * Issue the next sequence number.
     *
     * @return the next assigned sequence number
     */
    public long generateSeqNo() {
        return nextSeqNo.getAndIncrement();
    }

    /**
     * Marks the provided sequence number as seen and updates the max_seq_no if needed.
     */
    public void advanceMaxSeqNo(final long seqNo) {
        nextSeqNo.accumulateAndGet(seqNo + 1, Math::max);
    }

    /**
     * Marks the provided sequence number as processed and updates the processed checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as processed 在#43205，由TranslogWriter将此SeqNo的operation持久化到磁盘后，通知到LocalCheckpointTracker来调用此更新方法
     */
    public synchronized void markSeqNoAsProcessed(final long seqNo) {
        markSeqNo(seqNo, processedCheckpoint, processedSeqNo);
    }

    /**
     * Marks the provided sequence number as persisted and updates the checkpoint if possible.
     *
     * @param seqNo the sequence number to mark as persisted 在
     */
    public synchronized void markSeqNoAsPersisted(final long seqNo) {
        markSeqNo(seqNo, persistedCheckpoint, persistedSeqNo);
    }

    private void markSeqNo(final long seqNo, final AtomicLong checkPoint, final LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        // make sure we track highest seen sequence number
        advanceMaxSeqNo(seqNo);
        if (seqNo <= checkPoint.get()) {
            // this is possible during recovery where we might replay an operation that was also replicated
            return;
        }
        final CountedBitSet bitSet = getBitSetForSeqNo(bitSetMap, seqNo);
        final int offset = seqNoToBitSetOffset(seqNo); // 设置此seqNo对应的那个bit位置 为true， 然后将从checkpoint开始拉下的都提升上来
        bitSet.set(offset);
        if (seqNo == checkPoint.get() + 1) {
            updateCheckpoint(checkPoint, bitSetMap);//很可能是被并发调用（是吗？对吗？），不过能够被调用到一回就够了，会做掉所有的工作？
        }//看：commit 7f8e1454的注释
    }

    /**
     * The current checkpoint which can be advanced by {@link #markSeqNoAsProcessed(long)}.
     *
     * @return the current checkpoint
     */
    public long getProcessedCheckpoint() {
        return processedCheckpoint.get();
    }

    /**
     * The current persisted checkpoint which can be advanced by {@link #markSeqNoAsPersisted(long)}.
     *
     * @return the current persisted checkpoint
     */
    public long getPersistedCheckpoint() {
        return persistedCheckpoint.get();
    }

    /**
     * The maximum sequence number issued so far.
     *
     * @return the maximum sequence number
     */
    public long getMaxSeqNo() {
        return nextSeqNo.get() - 1;
    }

    /**
     * constructs a {@link SeqNoStats} object, using local state and the supplied global checkpoint
     *
     * This is needed to make sure the persisted local checkpoint and max seq no are consistent
     */
    public synchronized SeqNoStats getStats(final long globalCheckpoint) {
        return new SeqNoStats(getMaxSeqNo(), getPersistedCheckpoint(), globalCheckpoint);
    }

    /**
     * Waits for all operations up to the provided sequence number to complete.
     *
     * @param seqNo the sequence number that the checkpoint must advance to before this method returns
     * @throws InterruptedException if the thread was interrupted while blocking on the condition
     */
    @SuppressForbidden(reason = "Object#wait")
    public synchronized void waitForProcessedOpsToComplete(final long seqNo) throws InterruptedException {
        while (processedCheckpoint.get() < seqNo) {
            // notified by updateCheckpoint
            this.wait();
        }
    }

    /**
     * Checks if the given sequence number was marked as processed in this tracker.
     */
    public boolean hasProcessed(final long seqNo) {
        assert seqNo >= 0 : "invalid seq_no=" + seqNo;
        if (seqNo >= nextSeqNo.get()) {
            return false;
        }
        if (seqNo <= processedCheckpoint.get()) {
            return true;
        }
        final long bitSetKey = getBitSetKey(seqNo);
        final int bitSetOffset = seqNoToBitSetOffset(seqNo);
        synchronized (this) {
            // check again under lock
            if (seqNo <= processedCheckpoint.get()) {
                return true;
            }
            final CountedBitSet bitSet = processedSeqNo.get(bitSetKey);
            return bitSet != null && bitSet.get(bitSetOffset);
        }
    }

    /**
     * Moves the checkpoint to the last consecutively processed sequence number. This method assumes that the sequence number
     * following the current checkpoint is processed.
     */
    @SuppressForbidden(reason = "Object#notifyAll")
    private void updateCheckpoint(AtomicLong checkPoint, LongObjectHashMap<CountedBitSet> bitSetMap) {
        assert Thread.holdsLock(this);
        assert getBitSetForSeqNo(bitSetMap, checkPoint.get() + 1).get(seqNoToBitSetOffset(checkPoint.get() + 1))
            : "updateCheckpoint is called but the bit following the checkpoint is not set";
        try {
            // keep it simple for now, get the checkpoint one by one; in the future we can optimize and read words
            long bitSetKey = getBitSetKey(checkPoint.get());
            CountedBitSet current = bitSetMap.get(bitSetKey);
            if (current == null) { //已经被清空了
                // the bit set corresponding to the checkpoint has already been removed, set ourselves up for the next bit set
                assert checkPoint.get() % BIT_SET_SIZE == BIT_SET_SIZE - 1;
                current = bitSetMap.get(++bitSetKey); // 尝试处理下一个区间试试
            }
            do {
                checkPoint.incrementAndGet();
                /*
                 * The checkpoint always falls in the current bit set or we have already cleaned it; if it falls on the last bit of the
                 * current bit set, we can clean it.
                 */
                if (checkPoint.get() == lastSeqNoInBitSet(bitSetKey)) { //如果是最后一个了，就可以清空以节约空间
                    assert current != null;
                    final CountedBitSet removed = bitSetMap.remove(bitSetKey);
                    assert removed == current;
                    current = bitSetMap.get(++bitSetKey);
                }
            } while (current != null && current.get(seqNoToBitSetOffset(checkPoint.get() + 1)));//todo：但是为什么是循环，是因为
        } finally {//todo：是因为想要一次调用，可能可以把其他需要update的也同样做了吗？因为checkpoint在变化，所以如果能够拿到 current.get(seqNoToBitSetOffset(checkPoint.get() + 1))
            // notifies waiters in waitForProcessedOpsToComplete //todo：说明当下的checkpoint后面的一个值，也可以是true，即也被设置了，当然可以循环
            this.notifyAll();// 用于test case模拟，不用于生产
        }
    }

    private static long lastSeqNoInBitSet(final long bitSetKey) {
        return (1 + bitSetKey) * BIT_SET_SIZE - 1;
    }

    /**
     * Return the bit set for the provided sequence number, possibly allocating a new set if needed.
     *
     * @param seqNo the sequence number to obtain the bit set for
     * @return the bit set corresponding to the provided sequence number
     */
    private static long getBitSetKey(final long seqNo) {
        return seqNo / BIT_SET_SIZE;
    }

    private CountedBitSet getBitSetForSeqNo(final LongObjectHashMap<CountedBitSet> bitSetMap, final long seqNo) {
        assert Thread.holdsLock(this);
        final long bitSetKey = getBitSetKey(seqNo);
        final int index = bitSetMap.indexOf(bitSetKey);
        final CountedBitSet bitSet;
        if (bitSetMap.indexExists(index)) {
            bitSet = bitSetMap.indexGet(index);
        } else {
            bitSet = new CountedBitSet(BIT_SET_SIZE);
            bitSetMap.indexInsert(index, bitSetKey, bitSet);
        }
        return bitSet;
    }

    /**
     * Obtain the position in the bit set corresponding to the provided sequence number. The bit set corresponding to the sequence number
     * can be obtained via {@link #getBitSetForSeqNo(LongObjectHashMap, long)}.
     *
     * @param seqNo the sequence number to obtain the position for
     * @return the position in the bit set corresponding to the provided sequence number
     */
    private static int seqNoToBitSetOffset(final long seqNo) {
        return Math.toIntExact(seqNo % BIT_SET_SIZE);
    }

}
