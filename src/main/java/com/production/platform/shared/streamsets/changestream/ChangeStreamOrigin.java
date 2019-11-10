package com.production.platform.shared.streamsets.changestream;

import com.production.platform.shared.mongo.MongoUtils;
import com.production.platform.shared.mongo.StreamStatus;
import com.google.gson.JsonObject;
import com.mongodb.MongoClient;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.streamsets.pipeline.api.*;
import com.streamsets.pipeline.api.base.BasePushSource;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class ChangeStreamOrigin extends BasePushSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeStreamOrigin.class);
    private MongoClient client;

    private ExecutorService readersPool;
    private ExecutorService changeStream;

    private BlockingQueue<ChangeStreamDocument<Document>> eventQueue = new LinkedBlockingQueue<>();
    private List<Future<StreamStatus>> changeStreamStatuses;
    private Lock lock = new ReentrantLock();
    private volatile boolean isInvalidateEventCaught;
    private int batchCounter;

    @Override
    protected List<ConfigIssue> init() {
        isInvalidateEventCaught = false;
        batchCounter = 0;

        if (!getMongoSecurityConfig().isEnableAuthentication()) {
            client = MongoUtils.getClient(getMongoEndpoint());
        } else {
            try {
                client = MongoUtils.getClient(
                        getMongoEndpoint(),
                        getMongoSecurityConfig().getUsername().get(),
                        getMongoSecurityConfig().getPassword().get(),
                        getMongoSecurityConfig().getSourceDB().get());
            } catch (StageException e) {
                throw new IllegalArgumentException("Invalid security params", e);
            }
        }

        if (deleteResumeTokenOnStart()) {
            MongoUtils.deleteResumeToken(client, getResumeTokenDatabase(), getResumeTokenCollection(), getDatabase(), getCollection());
        }

        readersPool = Executors.newFixedThreadPool(getNumberOfThreads());
        changeStream = Executors.newFixedThreadPool(getCountOfReadThreads());
        return super.init();
    }

    @Override
    public void produce(Map<String, String> lastOffsets, int maxBatchSize) {
        changeStreamStatuses = createChangeStreamTaskList().stream()
                .map(changeStream::submit)
                .collect(Collectors.toList());

        List<Future<StreamStatus>> readerStatuses = Stream.generate(this::createRecordComputeTask)
                .limit(getNumberOfThreads())
                .map(readersPool::submit)
                .collect(Collectors.toList());

        for (Future<StreamStatus> readerStatus : readerStatuses) {
            try {
                StreamStatus status = readerStatus.get();
                LOGGER.error(status.toString());
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Reader thread has interrupted", e);
            }
        }

        for (Future<StreamStatus> changeStreamStatus : changeStreamStatuses) {
            try {
                StreamStatus status = changeStreamStatus.get();
                LOGGER.error(status.toString());
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.error("Change Stream thread has interrupted", e);
            }
        }

        if (isInvalidateEventCaught) {
            invalidate();
        }
    }


    @Override
    public void destroy() {
        terminate(readersPool);
        terminate(changeStream);
        super.destroy();
    }

    private Callable<StreamStatus> createRecordComputeTask() {
        return () -> {
            while (!getContext().isStopped() && !Thread.interrupted() && !isInvalidateEventCaught && isChangeStreamAlive()) {
                int recordAdded = 0;
                BatchContext batchContext = getContext().startBatch();
                BsonDocument resumeToken = null;
                while (recordAdded < getMongoBatchSize()) {
                    ChangeStreamDocument<Document> document = eventQueue.poll(getMongoWaitTime(), TimeUnit.MILLISECONDS);
                    if (document != null) {
                        if (document.getOperationType() == OperationType.INVALIDATE) {
                            isInvalidateEventCaught = true;
                            return new StreamStatus(getDatabase(), getCollection(), StreamStatus.Status.INVALIDATE_EVENT_CAUGHT);
                        }
                        resumeToken = document.getResumeToken();
                        Record record = createRecord(document);
                        batchContext.getBatchMaker().addRecord(record);
                        recordAdded++;
                    } else {
                        break;
                    }
                }
                if (recordAdded > 0) {
                    getContext().processBatch(batchContext);
                    lock.lock();
                    try {
                        batchCounter += recordAdded;
                        if (batchCounter >= getMongoBatchSize()) {
                            LOGGER.error("Sending resume token to Mongo: " + resumeToken.toString());
                            MongoUtils.sendResumeToken(client, getResumeTokenDatabase(), getResumeTokenCollection(), getDatabase(), getCollection(), resumeToken);
                            batchCounter = 0;
                        }
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage(), e);
                        throw new IllegalStateException(e);
                    } finally {
                        lock.unlock();
                    }
                }
            }
            return new StreamStatus(getDatabase(), getCollection(), StreamStatus.Status.STREAM_STOPPED);
        };
    }

    private List<Callable<StreamStatus>> createChangeStreamTaskList() {
        BsonDocument resumeToken = MongoUtils.getResumeToken(client, getResumeTokenDatabase(), getResumeTokenCollection(), getDatabase(), getCollection());

        if (resumeToken == null) {
            LOGGER.error("Resume token is null. Listening for the last changes");
        }

        List<Callable<StreamStatus>> tasks = new ArrayList<>();
        Bson fieldsToInclude = Aggregates.project(Projections.fields(Projections.include(getFilter())));
        if (isParallelRead()) {
            for (int threadNumber = 0; threadNumber < getCountOfReadThreads(); threadNumber++) {
                Bson partitionCriteria = Aggregates.match(Filters.mod(getPartitionField(), getCountOfReadThreads(), threadNumber));
                List<Bson> pipeline = Arrays.asList(fieldsToInclude, partitionCriteria);
                tasks.add(createChangeStreamTask(pipeline, resumeToken));
            }
        } else {
            tasks.add(createChangeStreamTask(Collections.singletonList(fieldsToInclude), resumeToken));
        }

        return tasks;
    }

    private Callable<StreamStatus> createChangeStreamTask(List<Bson> pipeline, BsonDocument resumeToken) {
        return MongoUtils.createStreamTask(getDatabase(), getCollection(), pipeline, getMongoBatchSize(), resumeToken, client,
                //Action to put record into queue
                //Queue with unrestricted capacity is used, so it's not needed to use 'put' method
                document -> eventQueue.add(document),
                //Predicate to stop change streams
                () -> !getContext().isStopped());
    }

    private void invalidate() {
        MongoUtils.deleteResumeToken(client, getResumeTokenDatabase(), getResumeTokenCollection(), getDatabase(), getCollection());
        LOGGER.error("Invalidate event caught from change stream on collection: "
                + getDatabase() + "." + getCollection());
        throw new IllegalStateException("Invalidate event caught");
    }

    private Record createRecord(ChangeStreamDocument<Document> document) {
        JsonObject record = new JsonObject();
        BsonValue documentKey = document.getDocumentKey().get("_id");
        String id = documentKey.isObjectId() ? documentKey.asObjectId().getValue().toString() : documentKey.asString().getValue();
        record.addProperty("id", id);
        record.addProperty("operation", document.getOperationType().getValue());
        record.addProperty("collection", document.getNamespace().getCollectionName());
        record.addProperty("timestamp", document.getClusterTime().getValue());

        Document fullDocument = document.getFullDocument();
        if (fullDocument != null) {
            record.addProperty("document", fullDocument.toJson());
        }
        return createSdcRecord(record.toString(), id);
    }

    private Record createSdcRecord(String json, String id) {
        Record record = getContext().createRecord(id);
        record.set("/", Field.create(new HashMap<>()));
        record.set("/document", Field.create(json));
        return record;
    }

    private boolean isChangeStreamAlive() {
        return changeStreamStatuses.stream().noneMatch(Future::isDone);
    }

    private void terminate(ExecutorService pool) {
        pool.shutdown();
        try {
            if (!pool.awaitTermination(3, TimeUnit.SECONDS)) {
                pool.shutdownNow();
            }
        } catch (InterruptedException e) {
            pool.shutdownNow();
        }
    }

    public abstract String getMongoEndpoint();

    public abstract String getDatabase();

    public abstract String getCollection();

    public abstract List<String> getFilter();

    public abstract int getMongoBatchSize();

    public abstract long getMongoWaitTime();

    public abstract int getNumberOfThreads();

    public abstract boolean deleteResumeTokenOnStart();

    public abstract boolean isParallelRead();

    public abstract int getCountOfReadThreads();

    public abstract String getPartitionField();

    public abstract String getResumeTokenDatabase();

    public abstract String getResumeTokenCollection();

    public abstract MongoSecurityConfig getMongoSecurityConfig();

}
