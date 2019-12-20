# WDPS1910 - Named Entity Linking
Web Data Processing System 2019 Assignment: Large Scale Entity Linking

##These are the steps involved in the assignment:

### Reading WARC File And Parsing HTML Content
Input to the assignment is a compressed WARC file, which is removed of the metadata and html entities of the web archives are parsed for the following entity generation.

### Named Entity Extraction
Possible entities are found using various NLP tools and parsers. These candidate entities are then named based on the label idetifying the semantics of the word. eg: Amsterdam is the capital of the Netherlands; here {Amsterdam, GPE} and {Netherlands, GPE} are the named entities which are labelled with GPE (Geopolitical Entity).

### Entity Linking To Freebase KB
The named entities are then searched with ElasticSearch to retrieve possible labels, freebase id and rank. The highest ranked freebase ids are selected for word sense disambiguation using sparql queries running in the trident server.
Best entries are matched based on a heuristics scoreline for similarity. The freebase id having the highest rank and having the highest connections in the knowledge base is fetched.

## Running The Code
There are two instances of python code, one with spark implementation `nerl_spark.py` and the other without spark `nerl.py`.
Make sure to clone this repository to the path of `/var/scratch/wdps1910/` if not yet done to run it in the DAS-4 cluster.
The following steps are to be ensured to run the solution:

### Start ElasticSearch Server
Make sure the ElasticSearch server is running, else run the script `start_elasticsearch_server.sh`
Do note the node and the port where its running.
These values should be give in the `config.py` under the key `ES_ADDRESS`. eg: `ES_ADDRESS = "node001:9200"`

### Start Trident Server
Start the trident server if not yet running using the script `start_sparql_server.sh`.
Similar to above, give the node and port configurations in `config.py`

### Running a regular instance
`python3.5 nerl.py <input_file> <output_file>`

### Running a pyspark instance
`spark-submit nerl_spark.py <input_file> <output_file> <spark_instance_name>`
A spark master instance name may be like: `spark://node013.cm.cluster:7077
`
Can be found out by viewing/curl the spark web UI or in the spark logs.

An alternative approach using HDFS, run `start_spark_run.sh`. Give appropriate values to the the arguements.

## Technologies
* Apache Spark (https://spark.apache.org/)
* Python 3.5.2 (https://www.python.org/)
* Trident (https://github.com/jrbn/trident)
* ElasticSearch (https://www.elastic.co/guide/en/elasticsearch/reference/2.4/index.html)