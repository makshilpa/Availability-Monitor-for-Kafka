//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package Microsoft.KafkaAvailability;

import Microsoft.KafkaAvailability.Metrics.AvailabilityGauge;
import Microsoft.KafkaAvailability.Metrics.MetricNameEncoded;
import Microsoft.KafkaAvailability.Metrics.SqlReporter;
import Microsoft.KafkaAvailability.Properties.AppProperties;
import Microsoft.KafkaAvailability.Properties.ConsumerProperties;
import Microsoft.KafkaAvailability.Properties.MetaDataManagerProperties;
import Microsoft.KafkaAvailability.Properties.ProducerProperties;
import com.google.gson.Gson;
import kafka.javaapi.TopicMetadata;
import org.apache.commons.cli.*;
import org.apache.log4j.MDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

/***
 * Sends a canary message to every topic and partition in Kafka.
 * Reads data from the tail of every topic and partition in Kafka
 * Reports the availability and latency metrics for the above operations.
 * Availability is defined as the percentage of total partitions that respond to each operation.
 */
public class App
{
    final static Logger logger = LoggerFactory.getLogger(App.class);
    static int sleepTime = 300000;
    static String cluster = "localhost";
    static MetricRegistry metrics;
    static AppProperties appProperties;

    public static void main(String[] args) throws IOException, MetaDataManagerException, InterruptedException
    {
        System.out.println("Starting KafkaAvailability Tool");
        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        appProperties = (AppProperties) appPropertiesManager.getProperties();
        Options options = new Options();
        options.addOption("r", "run", true, "Number of runs. Don't use this argument if you want to run infintely.");
        options.addOption("s", "sleep", true, "Time (in milliseconds) to sleep between each run. Default is 300000");
        Option clusterOption = Option.builder("c").hasArg().required(true).longOpt("cluster").desc("(REQUIRED) Cluster name").build();
        options.addOption(clusterOption);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try
        {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            int howManyRuns;

            cluster = line.getOptionValue("cluster");
            MDC.put("cluster", cluster);

            if (line.hasOption("sleep"))
            {
                sleepTime = Integer.parseInt(line.getOptionValue("sleep"));
            }
            if (line.hasOption("run"))
            {
                howManyRuns = Integer.parseInt(line.getOptionValue("run"));
                for (int i = 0; i < howManyRuns; i++)
                {
                    InitMetrics();
                    RunOnce();
                    Thread.sleep(sleepTime);
                }
            } else
            {
                while (true)
                {
                    InitMetrics();
                    RunOnce();
                    Thread.sleep(sleepTime);
                }
            }
        } catch (ParseException exp)
        {
            // oops, something went wrong
            System.err.println("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("KafkaAvailability", options);
        }

    }

    private static void InitMetrics()
    {
        metrics = new MetricRegistry();

        if (appProperties.reportToSlf4j)
        {
            final Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(metrics)
                    .outputTo(LoggerFactory.getLogger("KafkaMetrics.Raw"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            slf4jReporter.start(sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToSql)
        {
            final SqlReporter sqlReporter = SqlReporter.forRegistry(metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(appProperties.sqlConnectionString, cluster);
            sqlReporter.start(sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToJmx)
        {
            final JmxReporter jmxReporter = JmxReporter.forRegistry(metrics).build();
            jmxReporter.start();
        }
        if (appProperties.reportToConsole)
        {
            final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            consoleReporter.start(sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToCsv)
        {
            final CsvReporter csvReporter = CsvReporter.forRegistry(metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(appProperties.csvDirectory));
            csvReporter.start(sleepTime, TimeUnit.MILLISECONDS);
        }
    }

    private static void RunOnce() throws IOException, MetaDataManagerException
    {
        IPropertiesManager producerPropertiesManager = new PropertiesManager<ProducerProperties>("producerProperties.json", ProducerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IPropertiesManager consumerPropertiesManager = new PropertiesManager<ConsumerProperties>("consumerProperties.json", ConsumerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(metaDataPropertiesManager);
        IProducer producer = new Producer(producerPropertiesManager, metaDataManager);
        IConsumer consumer = new Consumer(consumerPropertiesManager, metaDataManager);

        int producerTryCount = 0;
        int producerFailCount = 0;
        long startTime, endTime;

        int numPartitions = metaDataManager.getAllTopicPartition().size();
        final SlidingWindowReservoir latency = new SlidingWindowReservoir(numPartitions);
        Histogram histogramProducerLatency = new Histogram(latency);
        MetricNameEncoded producerLatency = new MetricNameEncoded("Producer.Latency", "all");
        metrics.register(new Gson().toJson(producerLatency), histogramProducerLatency);


        for (kafka.javaapi.TopicMetadata item : metaDataManager.getAllTopicPartition())
        {
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramProducerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded producerTopicLatency = new MetricNameEncoded("Producer.Topic.Latency", item.topic());
            if (!metrics.getNames().contains(new Gson().toJson(producerTopicLatency)))
            {
                metrics.register(new Gson().toJson(producerTopicLatency), histogramProducerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {                
                MetricNameEncoded producerPartitionLatency = new MetricNameEncoded("Producer.Partition.Latency", item.topic() + part.partitionId());
                Histogram histogramProducerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!metrics.getNames().contains(new Gson().toJson(producerPartitionLatency)))
                {
                    metrics.register(new Gson().toJson(producerPartitionLatency),histogramProducerPartitionLatency);
                }
                try
                {
                    producerTryCount++;
                    startTime = System.currentTimeMillis();
                    producer.SendCanaryToTopicPartition(item.topic(), Integer.toString(part.partitionId()));
                    endTime = System.currentTimeMillis();
                    histogramProducerLatency.update(endTime - startTime);
                    histogramProducerTopicLatency.update(endTime - startTime);
                    histogramProducerPartitionLatency.update(endTime - startTime);
                } catch (Exception e)
                {
                    logger.error("Error Writing to Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    producerFailCount++;
                }

            }
        }
        MetricNameEncoded producerAvailability = new MetricNameEncoded("Producer.Availability", "all");
        metrics.register(new Gson().toJson(producerAvailability), new AvailabilityGauge(producerTryCount, producerTryCount - producerFailCount));

        int consumerTryCount = 0;
        int consumerFailCount = 0;
        Histogram histogramConsumerLatency = new Histogram(latency);

        MetricNameEncoded consumerLatency = new MetricNameEncoded("Consumer.Latency", "all");
        metrics.register(new Gson().toJson(consumerLatency), histogramConsumerLatency);

        for (kafka.javaapi.TopicMetadata item : metaDataManager.getAllTopicPartition())
        {
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramConsumerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded consumerTopicLatency = new MetricNameEncoded("Consumer.Topic.Latency", item.topic());
            if (!metrics.getNames().contains(new Gson().toJson(consumerTopicLatency)))
            {
                metrics.register(new Gson().toJson(consumerTopicLatency), histogramConsumerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {
                MetricNameEncoded consumerPartitionLatency = new MetricNameEncoded("Consumer.Partition.Latency", item.topic() + part.partitionId());
                Histogram histogramConsumerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!metrics.getNames().contains(new Gson().toJson(consumerPartitionLatency)))
                {
                    metrics.register(new Gson().toJson(consumerPartitionLatency),histogramConsumerPartitionLatency);
                }
                try
                {
                    consumerTryCount++;
                    startTime = System.currentTimeMillis();
                    consumer.ConsumeFromTopicPartition(item.topic(), part.partitionId());
                    endTime = System.currentTimeMillis();
                    histogramConsumerLatency.update(endTime - startTime);
                    histogramConsumerTopicLatency.update(endTime - startTime);
                    histogramConsumerPartitionLatency.update(endTime - startTime);
                } catch (Exception e)
                {
                    logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    consumerFailCount++;
                }

            }
        }
        MetricNameEncoded consumerAvailability = new MetricNameEncoded("Consumer.Availability", "all");
        metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(consumerTryCount, consumerTryCount - consumerFailCount));
    }
}
