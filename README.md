# Azure Cosmos DB - Performance testing using Mongo DB API support
This tool is used to verify/tune performance while using Azure Cosmos DB - Mongo Protocol support for a given collection.
It monitors and outputs the following metrics of interest:
1. Inserts per second
1. RUs (request units) per second 

(1) can be used to verify/tune if more/less performance is required as per expectations.
(2) can be used to confirm if the provisioned throughput is available for utilization.

## Getting Started

### Prerequisites

* Mongodb java driver 3.2.2

### Configuration

All configuration parameters are set via 'config' file in resources folder.
Here is the list of config parameters:

#### ThreadCount 
Number of threads to control parallelism

#### UserName 
Database account name from Azure Portal

#### Password 
Read write key from Azure Portal

#### AccEndpoint 
Database account endpoint from Azure Portal

#### Port
Mongo port from Azure Portal

#### DatabaseName
A name for database to be used for benchmarking

###### Note
If you have provision collection from portal, please use the same database name corresponding to that collection. 

#### CollectionName
A name for collection to be used for benchmarking

###### Note
If you have provision collection from portal, please use the same collection name. 

#### IsPCollection
True for MultiPartition collection or False for SinglePartition collection based on what type of collection must be  bench marked.

#### CollectionPartitionKeyField
partition key field name (this will be a dummy field if collection is single partition)

#### NumberOfDocumentsToInsert
Number of documents to be inserted in a single benchmark run

#### BatchSize
Number of documents to insert (bulk) as part of each operation

#### DocumentTemplateFile
Sample document file. 

##### Note
This can be replaced with custom file for more accurate analysis and performance tuning.

#### PreProvisioned
True if collection is provisioned already via portal. False if benchmark tool should create collection.

##### Note
If PreProvisioned is set to False, the collection created by benchmark tool will always be set to default RUs.
In order to use a collection of desired RUs for performance tuning, please set this to True and provision collection via Azure portal.
Make sure to use the correct Database and Collection names in the respective configs (DatabaseName, CollectionName).

Here is a sample config file:

```
ThreadCount=5
UserName=<<dbaccname>>
Password=<<password>>
AccEndpoint=<<endpoint>>
Port=10250
PreProvisioned=true
DatabaseName=perfTestDB
CollectionName=perfTestCollSP
IsPCollection=false
CollectionPartitionKeyField=partitionKey
NumberOfDocumentsToInsert=100000
BatchSize=24
DocumentTemplateFile=sample.json
```

### Customization

The tool uses a sample document to insert in the collection for measuring  performance.
This document is defined in sample.json file located in resources folder.
For accurate tuning, the content of this document can be replaced with an actual document that is closer to what will be stored in the collection.


### Tuning

In order to get started with performance tuning, here is handy table with configuration values to reach 'Target RUs' on specific types of collection.
These values were obtained by tuning using sample.json document for benchmarking.

Collection type | Target RUs | ThreadCount | Batchsize
------------ | ------------- | ------------- | ------------- 
Single Partition | 10000 | 5 | 24
Multi  Partition | 100000 | 55 | 24
Multi  Partition | 250000 | 134 | 24

### Running

Here is a portion of output running against single partition 10K RU collection:

```
Inserted 4920 docs @ 351.428558 writes/s, 9656.500000 RU/s
Inserted 5304 docs @ 353.600006 writes/s, 9716.466667 RU/s
Inserted 5664 docs @ 354.000000 writes/s, 9727.375000 RU/s
Inserted 6048 docs @ 355.764709 writes/s, 9775.705882 RU/s
Inserted 6432 docs @ 357.333344 writes/s, 9818.666667 RU/s
Inserted 6864 docs @ 361.263153 writes/s, 9926.736842 RU/s
Inserted 7248 docs @ 362.399994 writes/s, 9957.850000 RU/s
Inserted 7632 docs @ 363.428558 writes/s, 9986.000000 RU/s
Inserted 8016 docs @ 364.363647 writes/s, 10011.909091 RU/s
Inserted 8448 docs @ 367.304352 writes/s, 10092.478261 RU/s
Inserted 8832 docs @ 368.000000 writes/s, 10111.500000 RU/s
Inserted 9216 docs @ 368.640015 writes/s, 10129.280000 RU/s
Inserted 9576 docs @ 368.307678 writes/s, 10120.115385 RU/s
Inserted 9936 docs @ 368.000000 writes/s, 10111.111111 RU/s
Inserted 10080 docs @ 360.000000 writes/s, 9891.000000 RU/s

Summary:
--------------------------------------------------------------------- 
Inserted 10080 docs @ 360.000000 writes/s, 9891.000000 RU/s
--------------------------------------------------------------------- 
Azure Cosmos DB Mongo Benchmark completed successfully.
```


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management

## Authors

* **Vidhoon Viswanathan** - [vidhoonv](https://github.com/vidhoonv)

## Acknowledgments

* This tool is built using [Azure Cosmos DB Benchmark tool](https://github.com/Azure/azure-documentdb-dotnet/tree/master/samples/documentdb-benchmark) as reference
