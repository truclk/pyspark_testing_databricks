# pyspark_testing_databricks 

I'm starting to use pyspark in databricks. I have some doubt about how it's working for partition/option. As I did some search and found nothing realible. I would some test script to confirm these points.

# Question:
- When we use spark.readStream.format("kafka"), without and subscriber name and "startingOffsets" is "latest". How would it handle after restarting? How can it handle offset: latest per job (means you stop and start a job is it get the place where is stopped. Starting with the the latest offet of the kafka topic?

- When two jobs subscribe on one topic of Kakfa, will it have different offset or use the same offset.



## Experimental:


### Test 1:

#### Condition

- First run kafka_producer.py to produce message to topics.

- Wait few minutes to generate enough data.

- Start notebook job and check first time of data is matching with first data in topic.


#### Results:

- First message in Kafka 08.25.2022 10:19:07


- Start 
2022-08-25 05:48:05.065728

