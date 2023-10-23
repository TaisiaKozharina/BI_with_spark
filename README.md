# Business Intelligence task in Spark Scala for technical interview

## Tech stack: 
🟣 Spark v2.4.0 \
🟣 Scala v2.11.12 


## Tasks overview: 
1️⃣ Data Preprocessing:\
Input data is loaded from CSV files (located in resources folder). Data is processed by 1st preprocessing method (without any optimization techniques)\
\
2️⃣ Optimization:\
4 different methods are tested for performance optimization impact: caching, broadcast join, partitioning, and combined methods. Execution time of all 5 methods is compared. Optimization methods are packaged in Class "Preprocessing".\
\
3️⃣ Identifying Duplicates & 4️⃣ Deduplication:\
Using a key combination of "UserID", "TransactionAmount" and "Timestamp" duplicates are identified and filtered out from the dataframe, leaving a single records with unique parameters.\
\
5️⃣ Understanding the Duplicate Issue:\
The duplication might have occured due to various reasons, such as manual input error, system malfunction, network hickup, etc. Nevertheless, the duplicate rows can be detected by looking at the value of column "TransactionID" - duplicates have a prefix "DUP_". By experiments it was detected, that indeed all duplicates (with "DUP_" prefix) have identical records along the dataset. There was maximum 1 original per duplicate. Additionally, it was checked whether the filtered dataset (clipping "DUP_" records) has duplicates - it did not. Therefore from all duplicates, the one having valid ID was left in the dataset.\
\
6️⃣ Data Validation:\
Quantity of marked duplicates in the original dataset: 500
Quantity of removed duplicates by described strategy: 500
"Valid" records (without "DUP_" prefix in ID) in original dataset: 10 000
Cleaned dataset row count (after removing dupes): 10 000\
\
7️⃣ Logging and Monitoring:\
For logging, an AWS CloudWatched logging was introduced. Each time duplicates are removed from the dataset, quantity of removed records is logged to CloudWatcher (using personal account and personal credentials -> will most probably throw error if you run it locally without authenticating first). Logging method on client side is located in Class "Monitoring". Message in in JSON format, containing one field {"removedDupes":xxx}\
![image](https://github.com/TaisiaKozharina/BI_with_spark/assets/93377667/71296464-d14b-4085-bd60-e6c79bdb6171)
![image](https://github.com/TaisiaKozharina/BI_with_spark/assets/93377667/a9fbe153-1350-4a73-9fba-8a5b790829c5)
\
A CloudWatch Alarm was set with filter based on custom cumulative metric which parses logs from log group "DUPE_REMOVAL". Threshold was set to 600 removals per hour. After exceeding this limit, alarm fires:\
![image](https://github.com/TaisiaKozharina/BI_with_spark/assets/93377667/93fe0a53-b4bf-4da5-9143-4291888dc107)
![image](https://github.com/TaisiaKozharina/BI_with_spark/assets/93377667/2532b9b6-1829-42e7-8826-e84fa6ef1eca)
\
8️⃣ Upload the task results to git repository:\
This Git repo contains all code-related solutions for tasks.\
\
*To prepare tasks I used IntelliJ Idea IDE. To run project in IntelliJ, clone this repo, build project using sbt, then run ``` Main.scala ``` file.*
