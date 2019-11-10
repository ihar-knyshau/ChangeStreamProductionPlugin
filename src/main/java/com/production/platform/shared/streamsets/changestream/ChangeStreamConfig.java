package com.production.platform.shared.streamsets.changestream;

import com.streamsets.pipeline.api.*;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@StageDef(
        version = 1,
        label = " Change Stream",
        description = "Consume MongoDB Change Streams",
        recordsByRef = true,
        icon = "default.png",
        onlineHelpRefUrl = "",
        resetOffset = true
)
@ConfigGroups(Groups.ChangeStreamGroup.class)
@GenerateResourceBundle
public class ChangeStreamConfig extends ChangeStreamOrigin {

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Mongo endpoint",
            group = Groups.ChangeStream
    )
    public String mongoEndpoint;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "ZooKeeper endpoint",
            group = Groups.ChangeStream
    )
    public String zooKeeperEndpoint;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Database",
            group = Groups.ChangeStream
    )
    public String database;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Collection",
            group = Groups.ChangeStream
    )
    public String collection;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Filter (fields separated by ',')",
            defaultValue = "resumeToken, ns, documentKey, clusterTime, operationType, fullDocument.uuid",
            group = Groups.ChangeStream
    )
    public String filter;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            label = "Batch size",
            defaultValue = "5000",
            group = Groups.ChangeStream
    )
    public Integer batchSize;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            label = "Max await time",
            defaultValue = "3000",
            group = Groups.ChangeStream
    )
    public Long awaitTime;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            label = "Number of worker threads",
            defaultValue = "5",
            group = Groups.ChangeStream
    )
    public Integer workerParallelism;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            label = "Delete resume token",
            defaultValue = "false",
            group = Groups.ChangeStream
    )
    public Boolean deleteResumeToken;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Resume token database",
            defaultValue = "tokens",
            group = Groups.ChangeStream
    )
    public String resumeTokenDatabase;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Resume token collection",
            defaultValue = "tokens",
            group = Groups.ChangeStream
    )
    public String resumeTokenCollection;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.BOOLEAN,
            label = "Parallel read from change stream",
            defaultValue = "false",
            group = Groups.ChangeStream
    )
    public Boolean parallelRead;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.STRING,
            label = "Partition field",
            defaultValue = "fullDocument.DOI",
            group = Groups.ChangeStream,
            dependsOn = "parallelRead",
            triggeredByValue = {"true"}
    )
    public String partitionField;

    @ConfigDef(
            required = true,
            type = ConfigDef.Type.NUMBER,
            label = "Number of read threads",
            defaultValue = "1",
            group = Groups.ChangeStream,
            dependsOn = "parallelRead",
            triggeredByValue = {"true"}
    )
    public Integer countOfReadThreads;

    @ConfigDefBean(groups = {Groups.Security})
    public MongoSecurityConfig mongodbConfig;

    @Override
    public String getMongoEndpoint() {
        return mongoEndpoint;
    }

    @Override
    public String getDatabase() {
        return database;
    }

    @Override
    public String getCollection() {
        return collection;
    }

    @Override
    public List<String> getFilter() {
        return Arrays.stream(filter.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
    }

    @Override
    public int getMongoBatchSize() {
        return batchSize;
    }

    @Override
    public long getMongoWaitTime() {
        return awaitTime;
    }

    @Override
    public int getNumberOfThreads() {
        return workerParallelism;
    }

    @Override
    public boolean deleteResumeTokenOnStart() {
        return deleteResumeToken;
    }

    @Override
    public boolean isParallelRead() {
        return parallelRead;
    }

    @Override
    public int getCountOfReadThreads() {
        return countOfReadThreads;
    }

    @Override
    public String getPartitionField() {
        return partitionField;
    }

    @Override
    public MongoSecurityConfig getMongoSecurityConfig() {
        return mongodbConfig;
    }

    @Override
    public String getResumeTokenDatabase() {
        return resumeTokenDatabase;
    }

    @Override
    public String getResumeTokenCollection() {
        return resumeTokenCollection;
    }
}


