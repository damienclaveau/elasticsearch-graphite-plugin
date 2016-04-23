package org.elasticsearch.service.graphite;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.http.HttpStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.indexing.IndexingStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.percolator.stats.PercolateStats;
import org.elasticsearch.index.recovery.RecoveryStats;
import org.elasticsearch.index.suggest.stats.SuggestStats;
import org.elasticsearch.indices.breaker.AllCircuitBreakerStats;
import org.elasticsearch.indices.breaker.CircuitBreakerStats;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessStats;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.transport.TransportStats;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class GraphiteReporter {

    private static final ESLogger logger = ESLoggerFactory.getLogger(GraphiteReporter.class.getName());
    private Writer writer;
    private final String host;
    private final int port;
    private final String prefix;
    private final Pattern graphiteInclusionRegex;
    private final Pattern graphiteExclusionRegex;
    private final String timestamp;
    private final List<IndexShard> indexShards;
    private final NodeStats nodeStats;
    private final NodeIndicesStats nodeIndicesStats;


    public GraphiteReporter(String host, int port, String prefix, NodeIndicesStats nodeIndicesStats,
                            List<IndexShard> indexShards, NodeStats nodeStats,
                            Pattern graphiteInclusionRegex, Pattern graphiteExclusionRegex) {
        this.host = host;
        this.port = port;
        this.prefix = prefix;
        this.indexShards = indexShards;
        this.nodeStats = nodeStats;
        this.graphiteInclusionRegex = graphiteInclusionRegex;
        this.graphiteExclusionRegex = graphiteExclusionRegex;
        this.timestamp = Long.toString(System.currentTimeMillis() / 1000);
        this.nodeIndicesStats = nodeIndicesStats;
    }

    public void run() {
        Socket socket = null;
        try {
            socket = getSocket();
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            sendNodeIndicesStats();
            sendIndexShardStats();
            sendNodeStats();

            writer.flush();
        } catch (Exception e) {
            logException(e);
            flushWriter();
        } finally {
            closeSocket(socket);
            writer = null;
        }
    }

    private void sendNodeStats() {
        sendNodeFsStats(nodeStats.getFs());
        sendNodeHttpStats(nodeStats.getHttp());
        sendNodeJvmStats(nodeStats.getJvm());
        sendNodeOsStats(nodeStats.getOs());
        sendNodeProcessStats(nodeStats.getProcess());
        sendNodeTransportStats(nodeStats.getTransport());
        sendNodeThreadPoolStats(nodeStats.getThreadPool());
        sendNodeCircuitBreakerStats(nodeStats.getBreaker());
    }
    
    private void sendNodeCircuitBreakerStats(AllCircuitBreakerStats allCircuitBreakerStats) {
        String type = buildMetricName("node.circuitbreaker");
        for(CircuitBreakerStats breaker : allCircuitBreakerStats.getAllStats()) {
            String id = type + "." + breaker.getName();
            sendInt(id, "trippedCount", breaker.getTrippedCount());
            sendInt(id, "estimated", breaker.getEstimated());
            sendInt(id, "limit", breaker.getLimit());
            sendFloat(id, "overhead", breaker.getOverhead());
        }
    }
    
    private void sendNodeThreadPoolStats(ThreadPoolStats threadPoolStats) {
        String type = buildMetricName("node.threadpool");
        Iterator<ThreadPoolStats.Stats> statsIterator = threadPoolStats.iterator();
        while (statsIterator.hasNext()) {
            ThreadPoolStats.Stats stats = statsIterator.next();
            String id = type + "." + stats.getName();

            sendInt(id, "threads", stats.getThreads());
            sendInt(id, "queue", stats.getQueue());
            sendInt(id, "active", stats.getActive());
            sendInt(id, "rejected", stats.getRejected());
            sendInt(id, "largest", stats.getLargest());
            sendInt(id, "completed", stats.getCompleted());
        }
    }

    private void sendNodeTransportStats(TransportStats transportStats) {
        String type = buildMetricName("node.transport");
        sendInt(type, "serverOpen", transportStats.serverOpen());
        sendInt(type, "rxCount", transportStats.rxCount());
        sendInt(type, "rxSizeBytes", transportStats.rxSize().bytes());
        sendInt(type, "txCount", transportStats.txCount());
        sendInt(type, "txSizeBytes", transportStats.txSize().bytes());
    }

    private void sendNodeProcessStats(ProcessStats processStats) {
        String type = buildMetricName("node.process");

        sendInt(type, "openFileDescriptors", processStats.getOpenFileDescriptors());
        if (processStats.getCpu() != null) {
            sendInt(type + ".cpu", "percent", processStats.getCpu().getPercent());
            sendInt(type + ".cpu", "totalSeconds", processStats.getCpu().getTotal().seconds());
        }
        
        if (processStats.getMem() != null) {
            sendInt(type + ".mem", "totalVirtual", processStats.getMem().getTotalVirtual().bytes());
        }
    }

    private void sendNodeOsStats(OsStats osStats) {
        String type = buildMetricName("node.os");

        sendInt(type + ".cpu", "percent", osStats.getCpuPercent());
        sendFloat(type + ".load", "average", osStats.getLoadAverage());

        if (osStats.getMem() != null) {
            sendInt(type + ".mem", "totalBytes", osStats.getMem().getTotal().bytes());
            sendInt(type + ".mem", "freeBytes", osStats.getMem().getFree().bytes());
            sendInt(type + ".mem", "usedBytes", osStats.getMem().getUsed().bytes());
            sendInt(type + ".mem", "freePercent", osStats.getMem().getFreePercent());
            sendInt(type + ".mem", "usedPercent", osStats.getMem().getUsedPercent());
        }

        if (osStats.getSwap() != null) {
            sendInt(type + ".swap", "totalBytes", osStats.getSwap().getTotal().bytes());
            sendInt(type + ".swap", "freeBytes", osStats.getSwap().getFree().bytes());
            sendInt(type + ".swap", "usedBytes", osStats.getSwap().getUsed().bytes());
         }
    }

    private void sendNodeJvmStats(JvmStats jvmStats) {
        String type = buildMetricName("node.jvm");
        sendInt(type, "uptime", jvmStats.getUptime().seconds());

        // mem
        sendInt(type + ".mem", "heapCommitted", jvmStats.getMem().getHeapCommitted().bytes());
        sendInt(type + ".mem", "heapUsed", jvmStats.getMem().getHeapUsed().bytes());
        sendInt(type + ".mem", "nonHeapCommitted", jvmStats.getMem().getNonHeapCommitted().bytes());
        sendInt(type + ".mem", "nonHeapUsed", jvmStats.getMem().getNonHeapUsed().bytes());

        Iterator<JvmStats.MemoryPool> memoryPoolIterator = jvmStats.getMem().iterator();
        while (memoryPoolIterator.hasNext()) {
            JvmStats.MemoryPool memoryPool = memoryPoolIterator.next();
            String memoryPoolType = type + ".mem.pool." + memoryPool.getName();

            sendInt(memoryPoolType, "max", memoryPool.getMax().bytes());
            sendInt(memoryPoolType, "used", memoryPool.getUsed().bytes());
            sendInt(memoryPoolType, "peakUsed", memoryPool.getPeakUsed().bytes());
            sendInt(memoryPoolType, "peakMax", memoryPool.getPeakMax().bytes());
        }

        // threads
        sendInt(type + ".threads", "count", jvmStats.getThreads().getCount());
        sendInt(type + ".threads", "peakCount", jvmStats.getThreads().getPeakCount());

        // classes
        sendInt(type + ".classes", "total", jvmStats.getClasses().getTotalLoadedClassCount());
        sendInt(type + ".classes", "unloaded", jvmStats.getClasses().getUnloadedClassCount());
        sendInt(type + ".classes", "loaded", jvmStats.getClasses().getLoadedClassCount());
        
        // garbage collectors
        for (JvmStats.GarbageCollector collector : jvmStats.getGc().getCollectors()) {
            String id = type + ".gc." + collector.getName();
            sendInt(id, "collectionCount", collector.getCollectionCount());
            sendInt(id, "collectionTimeSeconds", collector.getCollectionTime().seconds());
        }
        
        // bufferPools
        Iterator<JvmStats.BufferPool>bufferPoolIterator = jvmStats.getBufferPools().iterator();
        while (bufferPoolIterator.hasNext()) {
            JvmStats.BufferPool bufferPool = bufferPoolIterator.next();
            String bufferPoolType = type + ".buffer.pool." + bufferPool.getName();

            sendInt(bufferPoolType, "totalCapacity", bufferPool.getTotalCapacity().bytes());
            sendInt(bufferPoolType, "used", bufferPool.getUsed().bytes());
            sendInt(bufferPoolType, "count", bufferPool.getCount());
        }
    }

    private void sendNodeHttpStats(HttpStats httpStats) {
        String type = buildMetricName("node.http");
        sendInt(type, "serverOpen", httpStats.getServerOpen());
        sendInt(type, "totalOpen", httpStats.getTotalOpen());
    }

    private void sendNodeFsStats(FsInfo fs) {
        Iterator<FsInfo.Path> infoIterator = fs.iterator();
        int i = 0;
        while (infoIterator.hasNext()) {
            String type = buildMetricName("node.fs") + i;
            FsInfo.Path info = infoIterator.next();
            sendInt(type, "available", info.getAvailable().bytes());
            sendInt(type, "total", info.getTotal().bytes());
            sendInt(type, "free", info.getFree().bytes());
            i++;
        }
    }

    private void sendIndexShardStats() {
        for (IndexShard indexShard : indexShards) {
            String shardType = indexShard.routingEntry().primary() ? ".primary" : ".replica";
            String type = buildMetricName("indexes.") + indexShard.shardId().index().name() + ".id." + indexShard.shardId().id() + shardType;
            sendIndexShardStats(type, indexShard);
        }
    }

    private void sendIndexShardStats(String type, IndexShard indexShard) {
        sendSearchStats(type + ".search", indexShard.searchStats());
        sendGetStats(type + ".get", indexShard.getStats());
        sendDocsStats(type + ".docs", indexShard.docStats());
        sendRefreshStats(type + ".refresh", indexShard.refreshStats());
        sendIndexingStats(type + ".indexing", indexShard.indexingStats("_all"));
        sendMergeStats(type + ".merge", indexShard.mergeStats());
        sendStoreStats(type + ".store", indexShard.storeStats());
        sendFieldDataStats(type + ".fielddata", indexShard.fieldDataStats());
        sendCompletionStats(type + ".completion", indexShard.completionStats());
        sendSuggestStats(type + ".suggest", indexShard.suggestStats());
        sendSegmentsStats(type + ".segments", indexShard.segmentStats());
        sendRecoveryStats(type + ".recovery", indexShard.recoveryStats());
        sendQueryCacheStats(type + ".querycache", indexShard.queryCacheStats());
        sendFlushStats(type + ".flush", indexShard.flushStats());
        sendWarmerStats(type + ".warmer", indexShard.warmerStats());
    }
    
    private void sendNodeIndicesStats() {
        String type = buildMetricName("indexes");
        sendSearchStats(type + ".search", nodeIndicesStats.getSearch());
        sendGetStats(type + ".get", nodeIndicesStats.getGet());
        sendDocsStats(type + ".docs", nodeIndicesStats.getDocs());
        sendRefreshStats(type + ".refresh", nodeIndicesStats.getRefresh());
        sendIndexingStats(type + ".indexing", nodeIndicesStats.getIndexing());
        sendMergeStats(type + ".merge", nodeIndicesStats.getMerge());
        sendStoreStats(type + ".store", nodeIndicesStats.getStore());
        sendFieldDataStats(type + ".fielddata", nodeIndicesStats.getFieldData());
        sendCompletionStats(type + ".completion", nodeIndicesStats.getCompletion());
        sendSuggestStats(type + ".suggest", nodeIndicesStats.getSuggest());
        sendSegmentsStats(type + ".segments", nodeIndicesStats.getSegments());
        sendRecoveryStats(type + ".recovery", nodeIndicesStats.getRecoveryStats());
        sendQueryCacheStats(type + ".querycache", nodeIndicesStats.getQueryCache());
        sendFlushStats(type + ".flush", nodeIndicesStats.getFlush());
        sendRequestCacheStats(type + ".requestcache", nodeIndicesStats.getRequestCache());
        sendPercolateStats(type + ".percolate", nodeIndicesStats.getPercolate());
    }

    private void sendStoreStats(String type, StoreStats storeStats) {
        sendInt(type, "sizeInBytes", storeStats.sizeInBytes());
        sendInt(type, "throttleTimeInNanos", storeStats.throttleTime().getNanos());
    }

    private void sendWarmerStats(String type, WarmerStats warmerStats) {
        sendInt(type, "current", warmerStats.current());
        sendInt(type, "total", warmerStats.total());
        sendInt(type, "totalTimeInMillis", warmerStats.totalTimeInMillis());
    }

    private void sendMergeStats(String type, MergeStats mergeStats) {
        sendInt(type, "total", mergeStats.getTotal());
        sendInt(type, "totalTimeInMillis", mergeStats.getTotalTimeInMillis());
        sendInt(type, "totalNumDocs", mergeStats.getTotalNumDocs());
        sendInt(type, "current", mergeStats.getCurrent());
        sendInt(type, "currentNumDocs", mergeStats.getCurrentNumDocs());
        sendInt(type, "currentSizeInBytes", mergeStats.getCurrentSizeInBytes());
    }

    private void sendCompletionStats(String type, CompletionStats completion) {
        sendInt(type, "sizeInBytes", completion.getSizeInBytes());
    }

    private void sendSuggestStats(String type, SuggestStats suggest) {
        sendInt(type, "count", suggest.getCount());
        sendInt(type, "current", suggest.getCurrent());
        sendInt(type, "timeInMillis", suggest.getTimeInMillis());
    }

    private void sendSegmentsStats(String type, SegmentsStats segments) {
        sendInt(type, "count", segments.getCount());
        sendInt(type, "memoryInBytes", segments.getMemoryInBytes());
        sendInt(type, "bitsetMemoryInBytes", segments.getBitsetMemoryInBytes());
        sendInt(type, "docValuesMemoryInBytes", segments.getDocValuesMemoryInBytes());
        sendInt(type, "indexWriterMaxMemoryInBytes", segments.getIndexWriterMaxMemoryInBytes());
        sendInt(type, "indexWriterMemoryInBytes", segments.getIndexWriterMemoryInBytes());
        sendInt(type, "normsMemoryInBytes", segments.getNormsMemoryInBytes());
        sendInt(type, "storedFieldsMemoryInBytes", segments.getStoredFieldsMemoryInBytes());
        sendInt(type, "termVectorsMemoryInBytes", segments.getTermVectorsMemoryInBytes());
        sendInt(type, "termsMemoryInBytes", segments.getTermsMemoryInBytes());
        sendInt(type, "versionMapMemoryInBytes", segments.getVersionMapMemoryInBytes());
    }

    private void sendRequestCacheStats(String type, RequestCacheStats requestCache) {
        sendInt(type, "evictions", requestCache.getEvictions());
        sendInt(type, "hitCount", requestCache.getHitCount());
        sendInt(type, "missCount", requestCache.getMissCount());
        sendInt(type, "memorySizeInBytes", requestCache.getMemorySizeInBytes());
    }

    private void sendRecoveryStats(String type, RecoveryStats recovery) {
        sendInt(type, "currentAsSource", recovery.currentAsSource());
        sendInt(type, "currentAsTarget", recovery.currentAsTarget());
    }

    private void sendQueryCacheStats(String type, QueryCacheStats queryCache) {
        sendInt(type, "evictions", queryCache.getEvictions());
        sendInt(type, "hitCount", queryCache.getHitCount());
        sendInt(type, "missCount", queryCache.getMissCount());
        sendInt(type, "memorySizeInBytes", queryCache.getMemorySizeInBytes());
        sendInt(type, "cacheCount", queryCache.getCacheCount());
        sendInt(type, "cacheSize", queryCache.getCacheSize());
        sendInt(type, "totalCount", queryCache.getTotalCount());
    }

    private void sendPercolateStats(String type, PercolateStats percolate) {
        sendInt(type, "count", percolate.getCount());
        sendInt(type, "current", percolate.getCurrent());
        sendInt(type, "memorySizeInBytes", percolate.getMemorySizeInBytes());
        sendInt(type, "numQueries", percolate.getNumQueries());
        sendInt(type, "timeInMillis", percolate.getTimeInMillis());
    }

    private void sendSearchStats(String type, SearchStats searchStats) {
        SearchStats.Stats totalSearchStats = searchStats.getTotal();
        sendSearchStatsStats(type + "._all", totalSearchStats);

        if (searchStats.getGroupStats() != null ) {
            for (Map.Entry<String, SearchStats.Stats> statsEntry : searchStats.getGroupStats().entrySet()) {
                sendSearchStatsStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendSearchStatsStats(String type, SearchStats.Stats searchStats) {
        sendInt(type, "queryCount", searchStats.getQueryCount());
        sendInt(type, "queryTimeInMillis", searchStats.getQueryTimeInMillis());
        sendInt(type, "queryCurrent", searchStats.getQueryCurrent());
        sendInt(type, "fetchCount", searchStats.getFetchCount());
        sendInt(type, "fetchTimeInMillis", searchStats.getFetchTimeInMillis());
        sendInt(type, "fetchCurrent", searchStats.getFetchCurrent());
        sendInt(type, "queryCurrent", searchStats.getQueryCurrent());
        sendInt(type, "scrollCount", searchStats.getScrollCount());
        sendInt(type, "scrollTimeInMillis", searchStats.getScrollTimeInMillis());
        sendInt(type, "scrollCurrent", searchStats.getScrollCurrent());
    }

    private void sendRefreshStats(String type, RefreshStats refreshStats) {
        sendInt(type, "total", refreshStats.getTotal());
        sendInt(type, "totalTimeInMillis", refreshStats.getTotalTimeInMillis());
    }

    private void sendIndexingStats(String type, IndexingStats indexingStats) {
        IndexingStats.Stats totalStats = indexingStats.getTotal();
        sendStats(type + "._all", totalStats);

        Map<String, IndexingStats.Stats> typeStats = indexingStats.getTypeStats();
        if (typeStats != null) {
            for (Map.Entry<String, IndexingStats.Stats> statsEntry : typeStats.entrySet()) {
                sendStats(type + "." + statsEntry.getKey(), statsEntry.getValue());
            }
        }
    }

    private void sendStats(String type, IndexingStats.Stats stats) {
        sendInt(type, "indexCount", stats.getIndexCount());
        sendInt(type, "indexTimeInMillis", stats.getIndexTimeInMillis());
        sendInt(type, "indexCurrent", stats.getIndexCount());
        sendInt(type, "deleteCount", stats.getDeleteCount());
        sendInt(type, "deleteTimeInMillis", stats.getDeleteTimeInMillis());
        sendInt(type, "deleteCurrent", stats.getDeleteCurrent());
    }

    private void sendGetStats(String type, GetStats getStats) {
        sendInt(type, "existsCount", getStats.getExistsCount());
        sendInt(type, "existsTimeInMillis", getStats.getExistsTimeInMillis());
        sendInt(type, "missingCount", getStats.getMissingCount());
        sendInt(type, "missingTimeInMillis", getStats.getMissingTimeInMillis());
        sendInt(type, "current", getStats.current());
    }

    private void sendFlushStats(String type, FlushStats flush) {
        sendInt(type, "total", flush.getTotal());
        sendInt(type, "totalTimeInMillis", flush.getTotalTimeInMillis());
    }

    private void sendDocsStats(String name, DocsStats docsStats) {
        sendInt(name, "count", docsStats.getCount());
        sendInt(name, "deleted", docsStats.getDeleted());
    }

    private void sendFieldDataStats(String name, FieldDataStats fieldDataStats) {
        sendInt(name, "memorySizeInBytes", fieldDataStats.getMemorySizeInBytes());
        sendInt(name, "evictions", fieldDataStats.getEvictions());
    }

    protected void sendToGraphite(String name, String value) {
        try {
            String nameToSend = sanitizeString(name);
            // check if this value is excluded
            if (graphiteExclusionRegex != null && graphiteExclusionRegex.matcher(nameToSend).matches()) {
                if (graphiteInclusionRegex == null ||
                    (graphiteInclusionRegex != null && !graphiteInclusionRegex.matcher(nameToSend).matches())) {
                    return;
                }
            }
            writer.write(nameToSend);
            writer.write(' ');
            writer.write(value);
            writer.write(' ');
            writer.write(timestamp);
            writer.write('\n');
            writer.flush();
        } catch (IOException e) {
            logger.error("Error sending to Graphite:", e);
        }
    }

    protected void sendInt(String name, String valueName, long value) {
        sendToGraphite(name + "." + valueName, String.format("%d", value));
    }

    protected void sendFloat(String name, String valueName, double value) {
        sendToGraphite(name + "." + valueName, String.format("%2.2f", value));
    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    protected String buildMetricName(String name) {
        return prefix + "." + name;
    }

    private void flushWriter() {
        if (writer != null) {
            try {
                writer.flush();
            } catch (IOException e1) {
                logger.error("Error while flushing writer:", e1);
            }
        }
    }

    public Socket getSocket() throws Exception {
        return new Socket(host, port);
    }

    private void closeSocket(Socket socket) {
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
                logger.error("Error while closing socket:", e);
            }
        }
    }

    private void logException(Exception e) {
        if (logger.isDebugEnabled()) {
            logger.debug("Error writing to Graphite", e);
        } else {
            logger.warn("Error writing to Graphite: {}", e.getMessage());
        }
    }
}
