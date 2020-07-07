###Suggested Architecture:

 S3  <--->  Spark EMR  --->  DynamoDB

---

Schedule a daily Spark job (using airflow or similar):
 1. read the data received since the last run
 2. aggregate per day, client id etc. and collects the activities & modules per such key (in this stage we are not doing a distinct count but rather collect the actual items we need).
 3. save those intermediate results to storage such as s3, partitioned by date
 4. now read the last 365 days of the intermediate data we created
 5. do the actual count distinct for every time frame requested
 6. push the results to a key-value DB, such as dynamoDB

---

- Late events. If we always read only the previous day, we will miss late events. we need to keep track of days that were not processed completely, and re-process them in the next run (either the entire day or keep track of minutes that were not processed yet. it's worth realizing if the events are always in order).
- Since we are expected to produce accurate results we can't use HHL or TS in the intermediate results. So we can keep the elements in a list, or we can use a bit array if there are really many) 
- The approach I suggested suits analytics dashboard better than real-time queries. For these, the specific use case is relevant. since we are reading 1-minute files this is not a real stream, so we need to consider how often the data should be updated. But basically - I think I would use Flink or something similar to aggregate the data in specific windows, and save the aggregation partitioned by client and date. Then query the data per specific client and dates. 
would probably want to run some compaction process daily to reduce storage and query time.

