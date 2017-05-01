DocumentDB - Mongo Protocol Support Perf Test tool
==================================================

This tool is used to verify/tune performance while using DocumentDB - Mongo Protocol support for a given collection.
It monitors and outputs the following metrics of interest:
1) Inserts per second
2) RUs (request units) per second 

(1) can be used to verify/tune if more/less performance is required as per expectations.
(2) can be used to confirm if the provisioned throughput is available for utilization.

Configuration
=============
All config parameters are set via 'config' file in resources folder.
Here is the list of config parameters:

ThreadCount=500
UserName=<<username>>
Password=<<password>>
AccEndpoint=<<DocDBMongoEndpoint>>
Port=10250
DatabaseName=<<DbName>>
CollectionName=<<CollName>>
IsPCollection=true
NumberOfDocumentsToInsert=100000
CollectionPartitionKeyField=partitionKey
DocumentTemplateFile=sample.json

sample.json
===========
The tool uses a sample document to insert in the collection for measuring  performance.
This document is defined in sample.json file located in resources folder.
For accurate tuning, the content of this document can be replaced with an actual document that is closer to what will be stored in the collection.
In the case of partitioned collection, we add a /partitionkey - GUID field as the partition key in addition to fields provided in the document.   