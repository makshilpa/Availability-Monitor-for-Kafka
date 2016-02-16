<h1>Kafka Availability Monitor </h1>

Kafka Availability monitor allows you to monitor the end to end availability and latency for sending and reading data from Kafka.
Producer Availability is defined as (Number of successful message sends accross all Topics and Partitions)/(Number of message send attempts)
Consumer Availability is defined as (Number of successfull tail data reads accross all Topics and Partitions)/(Number of read attempts)

The tool measures Producer availability and latency by sending canary messages to all partitions. The message prefix is "CanaryMessage_" and can be changed in the producerProperties.json resource file.
The tool measures Consumer availability and latency by reading data from the tail of all partitions.


## Usage
1. Clone the repository locally
	```
		git clone https://github.com/Microsoft/Kafka-Availability-Monitor.git
	```

2. Change the resource file(s) `KafkaAvailability / src / main / resources / metadatamanagerProperties.json` and add your zookeeperhost ip and ports
	```
		"zooKeeperHosts": "1.1.1.1:2181,2.2.2.2:2181",
	```
Also modify the following files to set up SQL/CSV/SLF4J/JMX reporting. The frequency of sending canary messages, reading tail data and the frequency of reporting are all controlled by the commandline argument "sleep" in milliseconds. The default value is 3000 milliseconds (ie. 5 mins).
	```
		KafkaAvailability / src / main / resources / log4j.properties
		KafkaAvailability / src / main / resources / appProperties.json
	```

3. If using precompiled jar, inject the modified resource file(s) into the jar
	```
		jar uf KafkaAvailability-1.0-SNAPSHOT-jar-with-dependencies.jar metadatamanagerProperties.json
	```
If assembling jar from source, if you want to report metrics to MS SQL Server, you must download and install the jdbc driver into your maven repo:
Visit the MSDN site for SQL Server and download the latest version of the JDBC driver for your operating system. Unzip the package. Open a command prompt and switch into the expanded directory where the jar file is located. Execute the following command. Be sure to modify the jar file name and version as necessary
	```
		mvn install:install-file -Dfile=sqljdbc4.jar -Dpackaging=jar -DgroupId=com.microsoft.sqlserver -DartifactId=sqljdbc4 -Dversion=4.0
	```
If you are not interested in SQL Server reporting, please remove the dependency on com.microsoft.sqlserver from the pom file before assembling the jar.
Finally, assemble jar using maven:
	```
		mvn clean compile assembly:single
	```

4. Run the assembled jar like this:
	```
		java.exe -jar KafkaAvailability-1.0-SNAPSHOT-jar-with-dependencies.jar -c ClusterName
	```
5. If you are logging to SQL Server, create tables in your db with the following names:
```
	[dbo].[Consumer.Availability]
	[dbo].[Consumer.Latency]
	[dbo].[Consumer.Topic.Latency]
	[dbo].[Consumer.Partition.Latency]
	[dbo].[Producer.Availability]
	[dbo].[Producer.Latency]
	[dbo].[Producer.Topic.Latency]
	[dbo].[Producer.Partition.Latency]
```
The availability and latency tables have the following schemas:
```
	CREATE TABLE [dbo].[Consumer.Availability](
	    [Environment] [varchar](100) NOT NULL,
		[Timestamp] [datetime] NOT NULL,
		[Tag] [varchar](100) NOT NULL,
		[Availability] [float] NOT NULL
	)
	CREATE TABLE [dbo].[Consumer.Latency](
		[Environment] [varchar](100) NOT NULL,
		[Timestamp] [datetime] NOT NULL,
		[Tag] [varchar](100) NOT NULL,
		[count] [float] NOT NULL,
		[max] [float] NOT NULL,
		[mean] [float] NOT NULL,
		[min] [float] NOT NULL,
		[stddev] [float] NOT NULL,
		[median] [float] NOT NULL,
		[75perc] [float] NOT NULL,
		[95perc] [float] NOT NULL,
		[98perc] [float] NOT NULL,
		[99perc] [float] NOT NULL,
		[999perc] [float] NOT NULL
	)
```
Errors can be logged using log4j (configure log4j.properties under resources) with this schema:
```
CREATE TABLE [dbo].[Errors](
	[USER_ID] [varchar](100) NOT NULL,
	[DATED] [datetime] NOT NULL,
	[LOGGER] [varchar](100) NOT NULL,
	[LEVEL] [varchar](100) NOT NULL,
	[MESSAGE] [varchar](max) NOT NULL
)
```

## Other Documentation

Please refer to JavaDoc for detailed code documentation.

## License

[![License](https://img.shields.io/badge/license-MIT-blue.svg?style=plastic)](https://github.com/Microsoft/Kafka-Availability-Monitor/blob/master/LICENCE.txt)

Kafka Availability Monitor is licensed under the MIT license. See [LICENSE](https://github.com/Microsoft/Kafka-Availability-Monitor/blob/master/LICENCE.txt) file for full license information.
