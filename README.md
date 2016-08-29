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

![Nifi Canvas Screenshot](https://github.com/swiftj/nifi-processors/blob/master/images/kinesis-processors.png)
    
## Bugs and Feedback

For bugs, questions and discussions please use the [Github Issues](https://github.com/swiftj/nifi-processors/issues).

## LICENSE

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

<http://www.apache.org/licenses/LICENSE-2.0>

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.