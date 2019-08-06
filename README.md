# SparkStreamingHBase
This example program, is example how to create Kafka Producer, Kafka Consumer, and insert streaming data to HBase using Java and Spark.
* Producer: reads hourly time series of humidity data (humidity.json) and sends to Kafka.
* Consumer: consumes data from Kafka, transforms, and saves to HBase.

<br />
<br />

## Details
### Kafka
* Kafka topic: test-v1
* Kafka topic partitions: 3

### HBase
* HBase table name: test-table-1
* HBase row id: city name (example: portland, san_francisco)
* HBase column family name: humidity
* HBase column qualifier name: datetime (example: 2017-11-29_23:00:00, 2017-11-30_00:00:00)
<table>
  <tr>
    <th rowspan="2"></th>
    <th colspan="4">humidity</th>
  </tr>
  <tr>
    <th>2017-11-29_23:00:00</th>
    <th>2017-11-30_00:00:00</th>
    <th>...</th>
    <th>yyyy-MM-dd_HH:mm:dd</th>
  </tr>
  <tr>
    <td>portland</td>
    <td>50.0</td>
    <td>60.5</td>
    <td>...</th>
    <td>45.0</td>
  </tr>
  <tr>
    <td>san_francisco</td>
    <td>55.0</td>
    <td>50.0</td>
    <td>...</th>
    <td>40.0</td>
  </tr>
</table>

### Program Arguments
#### Producer
args[0]
<pre>
{
  "kafka": {
    "broker": "host:port",
    "kafka_topic": "topic_name"
  }
}
</pre>

args[1]: path to JSON file

#### Consumer
args[0]
<pre>
{
  "hbase": {
    "output_table": "table_name",
    "master": "host",
    "quorum": "host",
    "hbaseZkPort":"port",
  },
  "kafka": {
    "broker": "host:port",
    "group_name": "group_name",
    "auto_offset_reset": "none/earliest/latest",
    "auto_commit": true/false,
    "kafka_topics": {
      "topic_name1": number_partitions,
      "topic_name2": number_partitions
    }
  }
}
</pre>

<br />
<br />


## How to compile
<pre>mvn install</pre>

<br />
<br />

## Running the program
### Producer
<pre>
spark-submit --class com.reza.dev.learn.main.Main_Producer --master local[2] target/SparkStreamingHBase-1.0-SNAPSHOT-jar-with-dependencies.jar '{"kafka":{"broker":"datanode01:6667,datanode02:6667,datanode03:6667","kafka_topic":"test"}}' '/doc/data/humidity.json'
</pre>

### Consumer
<pre>
spark-submit --class com.reza.dev.learn.main.Main_Consumer --master local[2] --conf spark.streaming.backpressure.enabled=true --conf spark.streaming.kafka.maxRatePerPartition=100 target/SparkStreamingHBase-1.0-SNAPSHOT-jar-with-dependencies.jar '{"hbase":{"output_table":"test-table-1","master":"namenode01,namenode02","quorum":"master,namenode01,namenode02","hbaseZkPort":"2181"},"kafka":{"broker":"datanode01:6667,datanode02:6667,datanode03:6667","group_name":"group-test","auto_offset_reset":"earliest","auto_commit":true,"kafka_topics":{"test-v1":3}}}'
</pre>
