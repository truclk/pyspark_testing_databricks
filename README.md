# pyspark_testing_databricks 

I'm starting to use pyspark in databricks. I have some doubt about how it's working for partition/option. As I did some search and found nothing realible. I would some test script to confirm these points.

# Question 1:
- When we use spark.readStream.format("kafka"), without and subscriber name and "startingOffsets" is "latest". How would it handle after restarting? How can it handle offset: latest per job (means you stop and start a job is it get the place where is stopped. Starting with the the latest offet of the kafka topic?

- When two jobs subscribe on one topic of Kakfa, will it have different offset or use the same offset.
- kafka_processor.py is used for this test

## Experimental:


### Test 1:

#### Condition

- First run kafka_producer.py to produce message to topics.
- Wait few minutes to generate enough data.
- Start notebook job with event_name metrics and check first time of data is matching with first data in topic.
- Stop and wait few minutes start it again, check for any data loss


#### Results:

- First message in Kafka 08.25.2022 10:19:07 (Local time UTC+7)
- First even in table 2022-08-25 05:48:05.065728 (UTC)
- Latest data before stop: 2022-08-25 06:07:16.213522
- Data after restart: 2022-08-25 06:10:36.54770


#### Conclusion:

- It always use latest offset of the topics


### Test 2:

#### Condition

- Doing test 1.
- Start nodebook job with event_name metadata 
- Checking it loss data or not


#### Results:

- First even in table of metadata `2022-08-25; 06:14:11.771; 2022-08-25; 06:14:06.877554; metadata; 2022-08-25`
- There is event in metrics table `2022-08-25 06:14:11.737; 2022-08-25 06:14:06.877554; metrics; 2022-08-25`

#### Conclusion:

- Subcriber of two topic is independent and we don't loss data for that.


# Question 2:

- We apply the group id and earliest startingOffsets `earliest` and do the same test of question 1
