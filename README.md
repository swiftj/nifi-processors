# Custom Apache Nifi Processors Project

## AWS Kinesis Processors for Apache Nifi

A couple of Apache Nifi Processors that use Amazon Web Services (Kinesis Streams)[https://aws.amazon.com/kinesis/streams/]. 
The intention is to use Apache Nifi data flows in the AWS cloud environment to stream data using Directed Acyclic Graphs 
constructed and composed within [Apache Nifi](https://nifi.apache.org/) to enrich and extend the capabilities and production
experience of using Apache Nifi which, currently, only supports AWS (Kinesis Firehose)[https://aws.amazon.com/kinesis/firehose/]
out of the box.

This project is built using Maven and generates a NAR file (as opposed to a JAR) that provides two custom FileFlow Nifi 
Processors that work exclusively with Kinesis Streams:

* GetKinesisStream
* PutKinesisStream

This NAR archive file must be copied to the Nifi installation directory in the ```libexec/lib`` subdirectory then start
or restart Apache Nifi and these two processors will be loaded. If loaded successfully, one can add them to the data
flow canvas in Apache Nifi:

[[https://github.com/swiftj/nifi-processors/tree/master/images/|alt=octocat]]



## Data Cleaning

The data that this web service expects is a list of category-subcategory name value pairs encoded in JSON. The list may 
contain duplicate category names provided their corresponding subcategories values are unique. For example, the 
following two categories are acceptable and are fully preserved as part of the cleaning process:

```
{ categories : ['PERSON', 'John'], ['PERSON', 'Steve'] }
```

However if both subcategory values are identical, this service will remove such duplicate pairs to ensure there is 
only one unique category-subcategory pair per cleaned output document. For example, assuming the following contrived
input data:

```
{ categories : ['PERSON', 'John'], ['PERSON', 'John'] }
```

Is effectively equivalent to the following:

```
{ categories : ['PERSON', 'John'] }
```
 
In short, only unique name-value pairs are preserved as part of the cleaned output. Similarly, as mentioned earlier,
any category-subcategory pair that does not have a matching category name from the master list of categories will 
result in the entire pair being ignored / removed by this service. For example, consider the following input:

```
{"categories": [["PERSON", "John"], ["ANIMAL", "Dog"], ["FOO", "Bar"], ["ANIMAL", "Bird"]]}
```

This web service will produce the following resulting output:

```
{ "categories": [["PERSON", "John"], ["ANIMAL", "Dog"], ["ANIMAL", "Bird"]], "counts": {"PERSON": 1, "ANIMAL": 2}}
```

Notice that this service tallies the total number of valid categories-subcategory pairs that it finds in the input 
data includes those counts in the output.

## Category Management

In addition to processing and cleaning input data, the kelso web service also provides the ability to add and delete 
categories from this master list. Additionally, this service will display the running total of counts for every 
category in this list. These counts are tallied across every client submission so they are aggregate values for all
submissions. The master list looks like the following at service startup:

```
{
  "COMPUTER": 0,
  "PERSON": 0,
  "PLACE": 0,
  "OTHER": 0,
  "ANIMAL": 0
}
```

## Setup, Deployment, and Testing

This service is written completely in [Python](https://www.python.org/) and utilizes the [Flask](http://flask.pocoo.org/)
microframework. You will need to install the Flask related dependencies either globally on your system before you can run
this service or you within your owh Python virtualenv (recommended). 

### Setup

It's recommended that you use virtualenv to run this service if you plan on doing so locally. If you don't have 
[virtualenv](http://docs.python-guide.org/en/latest/dev/virtualenvs/) already installed, execute the following command 
from your shell:

```
$ pip install virtualenv
```

Keep in mind that you will likely have to run this command under [sudo](http://en.wikipedia.org/wiki/Sudo) if your 
running on Linux or Mac platforms. Once you have virtualenv installed, you want to pull this project from GitHub into a 
local working directory and chdir into that directory.
 
Next, you want to create a new virtual environment by executing the following command from the source directory:

```
$ virtualenv env
```

This will create a new subdirectory called "env". Now you want to activate your environment:

```
$ source env/bin/activate
```

After you've activated your environment, you can safely add the following dependencies:

```
$ env/bin/pip install flask
$ env/bin/pip install flask-restful
$ env/bin/pip install flask-httpauth
```

Finally, you should be able to start the kelso service from the command line like so:

```
 $ env/bin/python service.py
  * Running on https://0.0.0.0:5000/ (Press CTRL+C to quit)
  * Restarting with stat
``` 

At this point, you may use your favorite REST client to start using the service located at the URL displayed at startup.
Note that you will need to present the proper credentials to access the service.

### Deployment

Currently, I am actively running this service within [Heroku](https://www.heroku.com) which is a terrific developer 
friendly cloud PaaS environment. I've taken the liberty of maintaining it there for those of you who don't want to 
bother with the setup and want to dive into using the system with a minimal effort as possible. The public webservice 
endpoint is:

```
https://floating-waters-7589.herokuapp.com
```

### Testing

The following table defines the URL space that amounts to the REST API purveyed by the kelso service.

```
+-------------+---------------------------------+----------------------------------------------------------------+
| HTTP Method | URI                             | Action                                                         |
+-------------+---------------------------------+----------------------------------------------------------------+
| GET         | /kelso/api/v1/categories        | Retrieves list of valid categories and their current counts    |
+-------------+---------------------------------+----------------------------------------------------------------+
| PUT         | /kelso/api/v1/categories/[name] | Adds a new category name or zeroes the count if already exists |
+-------------+---------------------------------+----------------------------------------------------------------+
| DELETE      | /kelso/api/v1/categories/[name] | Deletes an existing category from the list                     |
+-------------+---------------------------------+----------------------------------------------------------------+
| POST        | /kelso/api/v1/cleaner           | Submits data to be cleaned by this service which is returned   |
+-------------+---------------------------------+----------------------------------------------------------------+
```

For the purposes of the rest of this document, I will illustrate accessing this API using the instance I have deployed 
on Heroku. 

#### Category List Management

Here's an example of how you can display the current list of valid categories that the service will accept:
 
```
$ curl -k -u <username>:<password> https://floating-waters-7589.herokuapp.com/kelso/api/v1/categories
{
  "ANIMAL": 0,
  "COMPUTER": 0,
  "PLACE": 0,
  "PERSON": 0,
  "OTHER": 0
}
```

If you are interested in the current count for one of the valid categories, you can ask for the count explicitly:

```
$ curl -k -u <username>:<password> https://floating-waters-7589.herokuapp.com/kelso/api/v1/categories/person
{
  "count": 0
}
```

To add a new category (e.g. 'PHONE') to this list, you would execute a PUT transaction and optionally verify it is 
in the list:

```
$ curl -X PUT -k -u <username>:<password> https://floating-waters-7589.herokuapp.com/kelso/api/v1/categories/phone
{
  "message": "Successfully added category PHONE"
}

$ curl -k -u <username>:<password> https://floating-waters-7589.herokuapp.com/kelso/api/v1/categories
{
  "COMPUTER": 0, 
  "PLACE": 0, 
  "ANIMAL": 0, 
  "OTHER": 0, 
  "PERSON": 0, 
  "PHONE": 0
}
```

#### Data Cleaning

Data to be cleaned is expected to be encoded in a JSON document with a single object property called "categories" that
is an array of string arrays of two elements each which are the category-subcategory pairs, in that order. In other 
words, for each string array, index 0 is assumed to be the category name and index 1 is assumed to be the corresponding
subcategory value. Here's an example with some valid category pairs and some invalid category pairs as well as a 
duplicate pair. 

```
$ curl -k -u <username>:<password> -X POST -H 'Content-Type: application/json' \
    -d '{"categories": [["PERSON", "John"], ["ANIMAL", "Dog"], ["PERSON", "John"], ["FOO", "Bar"], \
                        ["animal", "Cat"], ["ANIMAL", "Bird", "extra"]]}' \
    https://floating-waters-7589.herokuapp.com/kelso/api/v1/cleaner
{
  "categories": [
    [
      "PERSON",
      "John"
    ],
    [
      "ANIMAL",
      "Dog"
    ],
    [
      "ANIMAL",
      "Cat"
    ],
    [
      "ANIMAL",
      "Bird"
    ]
  ],
  "counts": {
    "PERSON": 1,
    "ANIMAL": 3
  }  
}
```

Notice the counts property correctly reflects the valid counts of categories found in the original submission and the 
order of the categories has been preserved.

