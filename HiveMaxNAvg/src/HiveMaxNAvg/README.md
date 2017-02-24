
##Objective 
####To find the highest and average score of a class for packed data using Hive User Defined Aggregating Function(UDAF) and System Defined Function(SDF)

The format of the record is
 <class_id><student_id><score>

Note: class_id: 3 digits, student_id: 5 digits, score: 3 digits

Used these two files to test the program
##### file 1

00112345093

00212325094

00312315088

00112355100

##### file 2

00111345087

00212225097

00222325084

00312315088

00112355100

00312316078

##Prerequisites

Download the below mentioned jars and configure into project build path

hadoop-common-2.7.0.jar

hadoop-core-1.1.2.jar

commons.logging-1.1.1.jar

hadoop-hdfs-2.7.1.jar

hive-exec-0.13.0.jar

JRE System Library[JavaSE-1.7] or above


####User Defined Function Script:
Save this script as HiveMaxNAvgUDAF.hql

add jar s3://hivebucket1/scripts/HiveMaxNAvg.jar;
CREATE TEMPORARY FUNCTION maximum AS
'HiveMaxNAvg.Maximum';
CREATE TEMPORARY FUNCTION mean AS
'HiveMaxNAvg.Mean';
CREATE EXTERNAL TABLE IF NOT EXISTS
stud_record_udf_aws (
classid int, studentid int, score int
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.RegexSerDe' WITH
SERDEPROPERTIES (
"input.regex" = "(\\d{3})(\\d{5})(\\d{3})"
) LOCATION 's3://hivebucket1/Data/';
SELECT classid,maximum(score),mean(score) FROM
stud_record_udf_aws GROUP BY classid;

####System Defined Function Script:
Save this script as HiveMaxNAvgSDF.hql

CREATE EXTERNAL TABLE IF NOT EXISTS
stud_record_sdf_aws (
classid int, studentid int, score int
)
ROW FORMAT SERDE
'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
"input.regex" = "(\\d{3})(\\d{5})(\\d{3})"
) LOCATION 's3://hivebucket1/Data/';
SELECT classid, MAX(score), AVG(score) FROM
stud_record_sdf_aws GROUP BY classid;



##Project creation and execution steps

Write HiveMaxNAvg program in Eclipse and export it into a jar named as HiveMaxNAvg.jar.

####Setting up an EC2 key pair

login AWS account, Go to Services and select EC2 In EC2, Click on Key Pairs , create a Key Pair and store it in your local machine.

####Creating S3 Bucket

Go to Services and select S3. In S3, Create bucket by providing bucket name and region where you want to create your bucket as hivebucket1. Click on created bucket and create three folders named as Data, Logs and Scripts. Now upload appropriate files into the folders

a. Upload the HiveMaxNAvgSDF.hql and HiveMaxNAvgUDAF.hql file into Scripts folder

b. Upload the File1.txt and File2.txt file into Data folder

####Creating Cluster

Go to Services and Click on Elastic MapReduce in the AWS console management. Click on create cluster. Provide the cluster details

a. Give the Cluster name and select the Logs folder

b. Select the key pair which we have created

c. After completing all the details click on Create cluster

d. The cluster will start within 10-15 minutes

e. In Steps, select the Custom JAR and click on configure and provide the System Defined Function script location along with arguments details and save it

f. After completion go to the stdout location and we can see the output result.

g. Again in Steps, select the Custom JAR and click on configure and provide the User Defined Aggregating Function script location along with arguments details and save it

h. After completion go to the stdout location and we can see the output result.

##Result

Result will be stored in stdout location in Logs folder.
