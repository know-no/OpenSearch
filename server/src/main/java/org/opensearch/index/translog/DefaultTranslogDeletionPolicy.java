/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;
import java.util.List;

/**
 * Default implementation for the {@link TranslogDeletionPolicy}. Plugins can override the default behaviour
 * via the {@link org.opensearch.plugins.EnginePlugin#getCustomTranslogDeletionPolicyFactory()}.
 *
 * The default policy uses total number, size in bytes and maximum age for files.
 */

public class DefaultTranslogDeletionPolicy extends TranslogDeletionPolicy {
    private long retentionSizeInBytes;

    private long retentionAgeInMillis;

    private int retentionTotalFiles;
    // 分别对应 settings里设置的： 保留文件大小， 保留时长， 保留文件个数
    public DefaultTranslogDeletionPolicy(long retentionSizeInBytes, long retentionAgeInMillis, int retentionTotalFiles) {
        super();
        this.retentionSizeInBytes = retentionSizeInBytes;
        this.retentionAgeInMillis = retentionAgeInMillis;
        this.retentionTotalFiles = retentionTotalFiles;
    }

    @Override
    public synchronized long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) throws IOException {
        long minByLocks = getMinTranslogGenRequiredByLocks(); // 快照占用的，softDelete涉及的
        long minByAge = getMinTranslogGenByAge(readers, writer, retentionAgeInMillis, currentTime());//考虑保留时长，获取需要保留的最小Translog generation
        long minBySize = getMinTranslogGenBySize(readers, writer, retentionSizeInBytes);
        final long minByAgeAndSize;
        if (minBySize == Long.MIN_VALUE && minByAge == Long.MIN_VALUE) {
            // both size and age are disabled;
            minByAgeAndSize = Long.MAX_VALUE;
        } else {
            minByAgeAndSize = Math.max(minByAge, minBySize); // 根据max的值，比如有1 2 3， 但是1超时，2未超时，但是 2 + 3 超文件大小里，所以只能留3, 这样条件都不会被打破
        }
        long minByNumFiles = getMinTranslogGenByTotalFiles(readers, writer, retentionTotalFiles);
        return Math.min(Math.max(minByAgeAndSize, minByNumFiles), minByLocks);
    }

    @Override
    public synchronized void setRetentionSizeInBytes(long bytes) {
        retentionSizeInBytes = bytes;
    }

    @Override
    public synchronized void setRetentionAgeInMillis(long ageInMillis) {
        retentionAgeInMillis = ageInMillis;
    }

    @Override
    protected synchronized void setRetentionTotalFiles(int retentionTotalFiles) {
        this.retentionTotalFiles = retentionTotalFiles;
    }
}
