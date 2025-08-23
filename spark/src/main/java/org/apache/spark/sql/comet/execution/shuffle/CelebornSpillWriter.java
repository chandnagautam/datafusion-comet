package org.apache.spark.sql.comet.execution.shuffle;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;

/**
 * RssSpillWriter
 */
public abstract class CelebornSpillWriter extends SpillWriter {
    protected ShuffleClient shuffleClient;

    protected long doSpilling(byte[][] dataTypes,
            RowPartition rowPartition,
            ShuffleWriteMetricsReporter writeMetricsToUse,
            double preferDictionaryRatio,
            String compressionCodec,
            int compressionLevel,
            boolean tracingEnabled) {
        long[] addresses = rowPartition.getRowAddreses();
        int[] sizes = rowPartition.getRowSizes();

        long start = System.nanoTime();

        int batchSize = (int) CometConf.COMET_COLUMNAR_SHUFFLE_BATCH_SIZE().get();

        long result = nativeLib.writeToCeleborn(addresses, sizes, dataTypes, shuffleClient,
                preferDictionaryRatio, batchSize, compressionCodec, compressionLevel, tracingEnabled);

        rowPartition.reset();

        synchronized (writeMetricsToUse) {
            writeMetricsToUse.incWriteTime(System.nanoTime() - start);
            writeMetricsToUse.incRecordsWritten(addresses.length);
            writeMetricsToUse.incBytesWritten(result);
        }

        return result;
    }
}
