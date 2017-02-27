
##Objective 
####To find the highest score of a class for packed data using Pig User Defined Aggregating Function(UDAF) and System Defined Function(SDF) on AWS platform

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

pig-0.13.0.jar

JRE System Library[JavaSE-1.7] or above


####User Defined Function Script:
Save this script as HadoopPigFindMaxUDF.pig

REGISTER 's3://pigbucket1/scripts/HadoopPigFindMax.jar' 
rec = LOAD 's3://pigbucket1/data/'  
USING findMax.CutLoadFunc('1-3,4-8,9-11') 
AS (course:Int, std:Int, score:Int); 
groupedrec = GROUP rec by course PARALLEL 1; 
maxrec = FOREACH groupedrec GENERATE findMax.IntMax(rec.score); 
STORE maxrec INTO 's3://pigbucket1/results/OutputUDF';


####System Defined Function Script:
Save this script as HadoopPigFindMaxSDF.pig

REGISTER 's3://pigbucket1/scripts/HadoopPigFindMax.jar' 
rec = LOAD 's3://pigbucket1/data/'  
USING findMax.CutLoadFunc('1-3,4-8,9-11') 
AS (course:Int, std:Int, score:Int); 
groupedrec = GROUP rec by course PARALLEL 1; 
maxrec = FOREACH groupedrec GENERATE MAX(rec.score); 
STORE maxrec INTO 's3://pigbucket1/results/Output';


##Project creation and execution steps

Write HadoopPigFindMax program in Eclipse and export it into a jar named as HadoopPigFindMax.jar.

####Setting up an EC2 key pair

login AWS account, Go to Services and select EC2 In EC2, Click on Key Pairs , create a Key Pair and store it in your local machine.

####Creating S3 Bucket

Go to Services and select S3. In S3, Create bucket by providing bucket name and region where you want to create your bucket as hivebucket1. Click on created bucket and create three folders named as Data, Logs and Scripts. Now upload appropriate files into the folders

a. Upload the HadoopPigFindMaxUDF.pig and HadoopPigFindMaxSDF.pig file into Scripts folder

b. Upload the File1.txt and File2.txt file into Data folder

####Creating Cluster

Go to Services and Click on Elastic MapReduce in the AWS console management. Click on create cluster. Provide the cluster details

a. Give the Cluster name and select the Logs folder

b. Select the key pair which we have created

c. After completing all the details click on Create cluster

d. The cluster will start within 10-15 minutes

e. In Steps, select the Custom JAR and click on configure and provide the System Defined Function script location along with arguments details and save it

f. Again in Steps, select the Custom JAR and click on configure and provide the User Defined Aggregating Function script location along with arguments details and save it

Note : provide different file names for storing results for SDF and UDF


##Result

Result will be stored in result folder of S3 bucket location.
