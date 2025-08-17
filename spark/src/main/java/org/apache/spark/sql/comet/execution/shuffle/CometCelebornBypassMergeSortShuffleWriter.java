package org.apache.spark.sql.comet.execution.shuffle;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import org.apache.celeborn.client.ShuffleClient;
import org.apache.celeborn.common.CelebornConf;
import org.apache.spark.Partitioner;
import org.apache.spark.ShuffleDependency;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.scheduler.MapStatus;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter;
import org.apache.spark.shuffle.ShuffleWriter;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.celeborn.SparkUtils;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocator;
import org.apache.spark.shuffle.comet.CometShuffleMemoryAllocatorTrait;
import org.apache.spark.shuffle.sort.CometShuffleExternalSorter;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.TempShuffleBlockId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.None$;
import scala.Option;
import scala.Product2;
import scala.Tuple2;
import scala.collection.Iterator;

final class CometCelebornBypassMergeSortWriter<K, V> extends ShuffleWriter<K, V> {
    private static Logger logger = LoggerFactory.getLogger(CometCelebornBypassMergeSortWriter.class);
    private ShuffleClient shuffleClient;
    private Long celebornShuffleId;
    private Long mapId;
    private TaskMemoryManager memoryManager;
    private TaskContext taskContext;
    private CometCelebornBypassMergeSortHandle<K, V> handle;
    private CelebornConf celebornConf;
    private ShuffleWriteMetricsReporter writeMetrics;
    private SparkConf conf;
    private SerializerInstance serializer;
    private Partitioner partitioner;
    private CometShuffleMemoryAllocatorTrait allocator;

    private long[] partitionLengths;

    private boolean stopping;

    private MapStatus mapStatus;

    private CometCelebornWriter[] partitionWriters;

    private boolean isAsync;
    private int asyncThreadNum;

    private final ExecutorService threadPool;
    private int partitionNum;

    private boolean tracingEnabled;
    private StructType schema;

    CometCelebornBypassMergeSortWriter(ShuffleClient shuffleClient, Long celebornShuffleId, Long mapId,
            TaskMemoryManager memoryManager, TaskContext taskContext, CometCelebornBypassMergeSortHandle<K, V> handle,
            SparkConf conf, CelebornConf celebornConf,
            ShuffleWriteMetricsReporter writerMetrics) {
        this.shuffleClient = shuffleClient;
        this.celebornShuffleId = celebornShuffleId;
        this.mapId = mapId;
        this.memoryManager = memoryManager;
        this.taskContext = taskContext;
        this.handle = handle;
        this.writeMetrics = writerMetrics;
        this.celebornConf = celebornConf;
        this.conf = conf;
        ShuffleDependency<K, V, V> dep = handle.dependency();
        this.serializer = dep.serializer().newInstance();
        this.partitioner = dep.partitioner();
        this.partitionNum = partitioner.numPartitions();
        this.schema = ((CometShuffleDependency<?,?,?,>) dep).schema().get();
        this.isAsync = (boolean) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_ENABLED().get();
        this.asyncThreadNum = (int) CometConf$.MODULE$.COMET_COLUMNAR_SHUFFLE_ASYNC_THREAD_NUM().get();
        this.tracingEnabled = (boolean) CometConf$.MODULE$.COMET_TRACING_ENABLED().get();

        if (isAsync) {
            logger.info("Async shuffle writer enabled for celeborn");
            this.threadPool = ShuffleThreadPool.getThreadPool();
        } else {
            logger.info("Async shuffle writer *not* enabled for celeborn");
            this.threadPool = null;
        }
    }

    @Override
    public long[] getPartitionLengths() {
        return partitionLengths;
    }

    @Override
    public Option<MapStatus> stop(boolean success) {
        if (stopping) {
            return None$.empty();
        } else {
            stopping = true;

            if (success) {
                if (mapStatus == null) {
                    throw new IllegalStateException("Cannot call stop(true) without having called write()")
                }

                return Option.apply(mapStatus);
            } else {
                if (partitionWriters != null) {
                    try {
                        for (CometCelebornWriter writer : partitionWriters) {
                            writer.freeMemory();
                        }
                    } finally {
                        partitionWriters = null;
                    }
                }
            }
        }
        return None$.empty();
    }

    @Override
    public void write(Iterator<Product2<K, V>> records) throws IOException {
        assert (partitionWriters == null);

        try {
            if (!records.hasNext()) {
                BlockManagerId bmId = SparkEnv.get().blockManager().shuffleServerId();
                mapStatus = SparkUtils.createMapStatus(bmId, partitionLengths, mapId);

                return;
            }
            final long openStartTime = System.nanoTime();
            partitionWriters = new CometCelebornWriter[partitionNum];

            allocator = CometShuffleMemoryAllocator.getInstance(conf, memoryManager,
                    Math.min(CometShuffleExternalSorter.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()));
            for (int i = 0; i < partitionNum; i++) {
                CometCelebornWriter writer = new CometCelebornWriter(shuffleClient, allocator, taskContext, serializer,
                        schema, writeMetrics, isAsync, asyncThreadNum, threadPool, tracingEnabled);
                partitionWriters[i] = writer;
            }

            writeMetrics.incWriteTime(System.nanoTime() - openStartTime);

            long outputRows = 0;

            while (records.hasNext()) {
                outputRows += 1;

                final Product2<K, V> record = records.next();

                final K key = record._1();

                int partitionId = partitioner.getPartition(key);

                partitionWriters[partitionId].insertRow((UnsafeRow) record._2(), partitionId);

            }

            Native _native = new Native();
            if (tracingEnabled) {
                _native.logMemoryUsage("comet_shuffle_", allocator.getUsed());
            }

            long spillRecords = 0;

            for (int i = 0; i < partitionNum; i++) {
                CometCelebornWriter writer = partitionWriters[i];
            }

        } catch (Exception e) {
            // TODO: handle exception
        }
    }
}
