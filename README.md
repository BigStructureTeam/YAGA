# YAGA
Yet Another Geo Aggregator.

## Prerequisites
- Python PEP 8 (for checking code-style in local test)

```bash
  pip install pep8
```
- Python Py4J == 0.9.0 (for initiating Spark in local test)

```bash
  pip install -r requirements.txt
```
-   Spark 2.1.0 for hadoop 2.7 (used for processing of the data in our case) 

 See installation info [here](http://spark.apache.org)

-   Kafka 0.10.1  : 

See general installation info [here](https://kafka.apache.org/quickstart) or for macOs, install via homebrew.

-   Kafka Python library (to be able to interact with the Kafka server)

```bash
pip install kafka
```
  
## How do we launch it ?   

The setup of the whole system being pretty complex, let's try and simplify it : 

**Local machine**

- First open 3 terminal windows/tabs.
- On the first one, change directory to Kafka server directory
- On the second one, change directory to our PyGenerator repository.
- On the third one, change directory to this repository (YAGA)

Once everything is setup, in order to start the whole infrastructure, you need to : 

- Launch the Kafka server (in the first tab)

```bash
<kafka-location>/bin/zookeeper-server-start.sh <kafka-location>/zookeeper.properties & <kafka-location>/bin/kafka-server-start.sh <kafka-location>/server.properties
```

- Create a topic on Kafka : ours is named `geodata` (same tab)

```bash
 <kafka-location>/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic <topic>
```
- Then start our data generator, that sends data to the `geodata` topic (second tab)

```bash
python <generator location>/generator.py
```

- Start Spark with Spark submit, including our script in Spark Home and Kafka integration package for Spark Streaming (third tab)

```bash
<spark-location>/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.1.0 --master local[8] <YAGA-repository>/yaga/handle_geodata.py
```
## How do we Test ?

- For testing on your local machine, just run the command below:

```bash
./dev/run_tests
```

