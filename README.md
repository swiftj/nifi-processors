# Custom Apache Nifi Processors Project

## AWS Kinesis Processors for Apache Nifi

A couple of Apache Nifi Processors that use Amazon Web Services [Kinesis Streams](https://aws.amazon.com/kinesis/streams/). 
The intention is to use Apache Nifi data flows in the AWS cloud environment to stream data using Directed Acyclic Graphs 
constructed and composed within [Apache Nifi](https://nifi.apache.org/) to enrich and extend the capabilities and production
experience of using Apache Nifi which, currently, only supports AWS [Kinesis Firehose](https://aws.amazon.com/kinesis/firehose/)
out of the box.

This project is built using Maven and generates a NAR file (as opposed to a JAR) that provides two custom FileFlow Nifi 
Processors that work exclusively with Kinesis Streams:

* GetKinesisStream
* PutKinesisStream

This NAR archive file must be copied to the Nifi installation directory in the ```libexec/lib``` subdirectory then start
or restart Apache Nifi and these two processors will be loaded. If loaded successfully, one can add them to the data
flow canvas in Apache Nifi:

![Nifi Canvas Screenshot](https://github.com/swiftj/nifi-processors/tree/master/images/kinesis-processors.png)
    
