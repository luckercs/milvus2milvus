import io.milvus.v2.client.ConnectConfig;
import io.milvus.v2.client.MilvusClientV2;
import io.milvus.v2.common.IndexParam;
import io.milvus.v2.service.collection.request.CreateCollectionReq;
import io.milvus.v2.service.collection.request.DescribeCollectionReq;
import io.milvus.v2.service.collection.request.HasCollectionReq;
import io.milvus.v2.service.collection.response.DescribeCollectionResp;
import io.milvus.v2.service.database.request.CreateDatabaseReq;
import io.milvus.v2.service.database.request.DescribeDatabaseReq;
import io.milvus.v2.service.database.response.DescribeDatabaseResp;
import io.milvus.v2.service.index.request.CreateIndexReq;
import io.milvus.v2.service.index.request.DescribeIndexReq;
import io.milvus.v2.service.index.request.ListIndexesReq;
import io.milvus.v2.service.index.response.DescribeIndexResp;
import io.milvus.v2.service.partition.request.CreatePartitionReq;
import io.milvus.v2.service.partition.request.ListPartitionsReq;
import io.milvus.v2.service.utility.request.CreateAliasReq;
import io.milvus.v2.service.utility.request.ListAliasesReq;
import io.milvus.v2.service.utility.response.ListAliasResp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MilvusUtil {
    private String uri;
    private String token;

    public MilvusUtil(String uri, String token) {
        this.uri = uri;
        this.token = token;
    }

    public MilvusClientV2 getMilvusClient(String dbName) {
        ConnectConfig connectConfig = ConnectConfig.builder()
                .uri(uri)
                .token(token)
                .dbName(dbName)
                .connectTimeoutMs(10000)
                .build();
        return new MilvusClientV2(connectConfig);
    }

    public List<String> getAllDbs() {
        MilvusClientV2 milvusClient = getMilvusClient("default");
        List<String> databaseNames = milvusClient.listDatabases().getDatabaseNames();
        milvusClient.close();
        return databaseNames;
    }

    public List<String> getAllCollections(String dbName) {
        MilvusClientV2 milvusClient = getMilvusClient(dbName);
        List<String> collectionNames = milvusClient.listCollections().getCollectionNames();
        milvusClient.close();
        return collectionNames;
    }

    public boolean hasCollection(String dbName, String collectionName) {
        MilvusClientV2 milvusClient = getMilvusClient(dbName);
        Boolean res = milvusClient.hasCollection(HasCollectionReq.builder().collectionName(collectionName).build());
        milvusClient.close();
        return res;
    }

    public Map<String, String> getDataBaseSchema(String dbName) {
        MilvusClientV2 milvusClient = getMilvusClient(dbName);
        DescribeDatabaseResp describeDatabaseResp = milvusClient.describeDatabase(DescribeDatabaseReq.builder().databaseName(dbName).build());
        Map<String, String> properties = describeDatabaseResp.getProperties();
        milvusClient.close();
        return properties;
    }

    public void createDatabase(String dbName, Map<String, String> properties) {
        MilvusClientV2 milvusClient = getMilvusClient("default");
        if (properties == null) {
            milvusClient.createDatabase(CreateDatabaseReq.builder().databaseName(dbName).build());
        } else {
            milvusClient.createDatabase(CreateDatabaseReq.builder().databaseName(dbName).properties(properties).build());
        }
        milvusClient.close();
    }


    public void createCollectionFromExistsCollection(MilvusClientV2 milvusClientSrc, MilvusClientV2 milvusClientTarget, String dbName, String dbNameTarget, String collectionName, boolean skip_index) {
        DescribeCollectionResp describeCollectionResp = milvusClientSrc.describeCollection(DescribeCollectionReq.builder().databaseName(dbName).collectionName(collectionName).build());
        Boolean hasPartitionKey = false;
        List<CreateCollectionReq.FieldSchema> fieldSchemaList = describeCollectionResp.getCollectionSchema().getFieldSchemaList();
        for (CreateCollectionReq.FieldSchema fieldSchema : fieldSchemaList) {
            if (fieldSchema.getIsPartitionKey()) {
                hasPartitionKey = true;
                break;
            }
        }

        if (hasPartitionKey) {
            milvusClientTarget.createCollection(CreateCollectionReq.builder().databaseName(dbNameTarget).collectionName(collectionName)
                    .collectionSchema(describeCollectionResp.getCollectionSchema())
                    .autoID(describeCollectionResp.getAutoID())
                    .consistencyLevel(describeCollectionResp.getConsistencyLevel())
                    .description(describeCollectionResp.getDescription())
                    .enableDynamicField(describeCollectionResp.getEnableDynamicField())
                    .numShards(describeCollectionResp.getShardsNum())
                    .properties(describeCollectionResp.getProperties())
                    .numPartitions(describeCollectionResp.getNumOfPartitions().intValue())
                    .build());
        } else {
            milvusClientTarget.createCollection(CreateCollectionReq.builder().databaseName(dbNameTarget).collectionName(collectionName)
                    .collectionSchema(describeCollectionResp.getCollectionSchema())
                    .autoID(describeCollectionResp.getAutoID())
                    .consistencyLevel(describeCollectionResp.getConsistencyLevel())
                    .description(describeCollectionResp.getDescription())
                    .enableDynamicField(describeCollectionResp.getEnableDynamicField())
                    .numShards(describeCollectionResp.getShardsNum())
                    .properties(describeCollectionResp.getProperties())
                    .build());
        }

        for (String partitionName : milvusClientSrc.listPartitions(ListPartitionsReq.builder().collectionName(collectionName).build())) {
            if (!partitionName.equals("_default")) {
                milvusClientTarget.createPartition(CreatePartitionReq.builder().collectionName(collectionName).partitionName(partitionName).build());
            }
        }

        ListAliasResp listAliasResp = milvusClientSrc.listAliases(ListAliasesReq.builder().collectionName(collectionName).build());
        List<String> alias = listAliasResp.getAlias();
        if (alias != null && !alias.isEmpty()) {
            for (String aliasName : alias) {
                milvusClientTarget.createAlias(CreateAliasReq.builder().collectionName(collectionName).alias(aliasName).build());
            }
        }

        if (!skip_index) {
            List<String> Indexes = milvusClientSrc.listIndexes(ListIndexesReq.builder().collectionName(collectionName).build());
            if (Indexes != null && !Indexes.isEmpty()) {
                List<IndexParam> indexParams = new ArrayList<>();
                for (String indexName : Indexes) {
                    DescribeIndexResp describeIndexResp = milvusClientSrc.describeIndex(DescribeIndexReq.builder().databaseName(dbName).collectionName(collectionName).indexName(indexName).build());
                    DescribeIndexResp.IndexDesc indexSrc = describeIndexResp.getIndexDescByIndexName(indexName);
                    IndexParam indexParam = IndexParam.builder().fieldName(indexSrc.getFieldName()).indexName(indexName).indexType(indexSrc.getIndexType()).extraParams(Collections.unmodifiableMap(indexSrc.getExtraParams())).metricType(indexSrc.getMetricType()).build();
                    indexParams.add(indexParam);
                }
                milvusClientTarget.createIndex(CreateIndexReq.builder().databaseName(dbNameTarget).collectionName(collectionName).indexParams(indexParams).build());
            }
        }
    }
}
