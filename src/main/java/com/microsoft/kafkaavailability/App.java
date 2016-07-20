//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability;

import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.metrics.SqlReporter;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.ConsumerProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ProducerProperties;
import com.google.gson.Gson;
import org.apache.commons.cli.*;
import org.apache.log4j.MDC;
import org.apache.log4j.net.SyslogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.codahale.metrics.*;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.List;

/***
 * Sends a canary message to every topic and partition in Kafka.
 * Reads data from the tail of every topic and partition in Kafka
 * Reports the availability and latency metrics for the above operations.
 * Availability is defined as the percentage of total partitions that respond to each operation.
 */
public class App
{
    final static Logger m_logger = LoggerFactory.getLogger(App.class);
    static int m_sleepTime = 120000;
    static String m_cluster = "localhost";
    static MetricRegistry m_metrics;
    static AppProperties appProperties;

    public static void main(String[] args) throws IOException, MetaDataManagerException, InterruptedException
    {
        m_logger.info("Starting KafkaAvailability Tool");
        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        appProperties = (AppProperties) appPropertiesManager.getProperties();
        Options options = new Options();
        options.addOption("r", "run", true, "Number of runs. Don't use this argument if you want to run infintely.");
        options.addOption("s", "sleep", true, "Time (in milliseconds) to sleep between each run. Default is 120000");
        Option clusterOption = Option.builder("c").hasArg().required(true).longOpt("cluster").desc("(REQUIRED) Cluster name").build();
        options.addOption(clusterOption);
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        try
        {
            // parse the command line arguments
            CommandLine line = parser.parse(options, args);
            int howManyRuns;

            m_cluster = line.getOptionValue("cluster");
            MDC.put("cluster", m_cluster);

            if (line.hasOption("sleep"))
            {
                m_sleepTime = Integer.parseInt(line.getOptionValue("sleep"));
            }
            if (line.hasOption("run"))
            {
                howManyRuns = Integer.parseInt(line.getOptionValue("run"));
                for (int i = 0; i < howManyRuns; i++)
                {
                    InitMetrics();
                    RunOnce();
                    Thread.sleep(m_sleepTime);
                }
            } else
            {
                while (true)
                {
                    InitMetrics();
                    RunOnce();
                    Thread.sleep(m_sleepTime);
                }
            }
        } catch (ParseException exp)
        {
            // oops, something went wrong
            m_logger.error("Parsing failed.  Reason: " + exp.getMessage());
            formatter.printHelp("KafkaAvailability", options);
        }

    }

    private static void InitMetrics()
    {
        m_metrics = new MetricRegistry();

        if (appProperties.reportToSlf4j)
        {
            final Slf4jReporter slf4jReporter = Slf4jReporter.forRegistry(m_metrics)
                    .outputTo(LoggerFactory.getLogger("KafkaMetrics.Raw"))
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            slf4jReporter.start(m_sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToSql)
        {
            final SqlReporter sqlReporter = SqlReporter.forRegistry(m_metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(appProperties.sqlConnectionString, m_cluster);
            sqlReporter.start(m_sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToJmx)
        {
            final JmxReporter jmxReporter = JmxReporter.forRegistry(m_metrics).build();
            jmxReporter.start();
        }
        if (appProperties.reportToConsole)
        {
            final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(m_metrics)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build();
            consoleReporter.start(m_sleepTime, TimeUnit.MILLISECONDS);
        }
        if (appProperties.reportToCsv)
        {
            final CsvReporter csvReporter = CsvReporter.forRegistry(m_metrics)
                    .formatFor(Locale.US)
                    .convertRatesTo(TimeUnit.SECONDS)
                    .convertDurationsTo(TimeUnit.MILLISECONDS)
                    .build(new File(appProperties.csvDirectory));
            csvReporter.start(m_sleepTime, TimeUnit.MILLISECONDS);
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

        int producerTryCount = 0, clusterIPStatusTryCount = 0, gtmIPStatusTryCount = 0;
        int producerFailCount = 0, clusterIPStatusFailCount = 0, gtmIPStatusFailCount = 0;
        long startTime, endTime;
        int numPartitions = 0;
        //Auto creating a whitelisted topics, if not available.
        metaDataManager.createWhiteListedTopics();
        m_logger.info("get metadata size");
        for (kafka.javaapi.TopicMetadata topic : metaDataManager.getAllTopicPartition())
        {
            numPartitions += topic.partitionsMetadata().size();
        }
        m_logger.info("done getting metadata size");

        final SlidingWindowReservoir latency = new SlidingWindowReservoir(numPartitions);
        Histogram histogramProducerLatency = new Histogram(latency);
        MetricNameEncoded producerLatency = new MetricNameEncoded("Producer.Latency", "all");
        if (appProperties.sendProducerLatency)
            m_metrics.register(new Gson().toJson(producerLatency), histogramProducerLatency);
        m_logger.info("start topic partition loop");

        for (kafka.javaapi.TopicMetadata item : metaDataManager.getAllTopicPartition())
        {
            m_logger.info("Starting VIP prop check." + appProperties.reportKafkaIPAvailability);
            if (appProperties.reportKafkaIPAvailability)
            {
                try
                {
                    m_logger.info("Starting VIP check.");
                    clusterIPStatusTryCount++;
                    producer.SendCanaryToKafkaIP(appProperties.kafkaClusterIP, item.topic(), false);
                } catch (Exception e)
                {
                    m_logger.info("VIP check exception");
                    clusterIPStatusFailCount++;
                    m_logger.error("ClusterIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                }
                try
                {
                    gtmIPStatusTryCount++;
                    producer.SendCanaryToKafkaIP(appProperties.kafkaGTMIP, item.topic(), false);
                } catch (Exception e)
                {
                    gtmIPStatusFailCount++;
                    m_logger.error("GTMIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                }
            }
            m_logger.info("done with VIP prop check.");
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramProducerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded producerTopicLatency = new MetricNameEncoded("Producer.Topic.Latency", item.topic());
            if (!m_metrics.getNames().contains(new Gson().toJson(producerTopicLatency)))
            {
                if (appProperties.sendProducerTopicLatency)
                    m_metrics.register(new Gson().toJson(producerTopicLatency), histogramProducerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {
                MetricNameEncoded producerPartitionLatency = new MetricNameEncoded("Producer.Partition.Latency", item.topic() + part.partitionId());
                Histogram histogramProducerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!m_metrics.getNames().contains(new Gson().toJson(producerPartitionLatency)))
                {
                    if (appProperties.sendProducerPartitionLatency)
                        m_metrics.register(new Gson().toJson(producerPartitionLatency), histogramProducerPartitionLatency);
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
                    m_logger.error("Error Writing to Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    producerFailCount++;
                }
            }
        }
        if (appProperties.reportKafkaIPAvailability)
        {
            m_logger.info("About to report kafkaIPAvail:" + clusterIPStatusTryCount + " " + clusterIPStatusFailCount);
            MetricNameEncoded kafkaClusterIPAvailability = new MetricNameEncoded("KafkaIP.Availability", "all");
            m_metrics.register(new Gson().toJson(kafkaClusterIPAvailability), new AvailabilityGauge(clusterIPStatusTryCount, clusterIPStatusTryCount - clusterIPStatusFailCount));
            MetricNameEncoded kafkaGTMIPAvailability = new MetricNameEncoded("KafkaGTMIP.Availability", "all");
            m_metrics.register(new Gson().toJson(kafkaGTMIPAvailability), new AvailabilityGauge(gtmIPStatusTryCount, gtmIPStatusTryCount - gtmIPStatusFailCount));
        }
        if (appProperties.sendProducerAvailability)
        {
            MetricNameEncoded producerAvailability = new MetricNameEncoded("Producer.Availability", "all");
            m_metrics.register(new Gson().toJson(producerAvailability), new AvailabilityGauge(producerTryCount, producerTryCount - producerFailCount));
        }
        int consumerTryCount = 0;
        int consumerFailCount = 0;
        Histogram histogramConsumerLatency = new Histogram(latency);

        if (appProperties.sendConsumerLatency)
        {
            MetricNameEncoded consumerLatency = new MetricNameEncoded("Consumer.Latency", "all");
            m_metrics.register(new Gson().toJson(consumerLatency), histogramConsumerLatency);
        }
        for (kafka.javaapi.TopicMetadata item : metaDataManager.getAllTopicPartition())
        {
            final SlidingWindowReservoir topicLatency = new SlidingWindowReservoir(item.partitionsMetadata().size());
            Histogram histogramConsumerTopicLatency = new Histogram(topicLatency);
            MetricNameEncoded consumerTopicLatency = new MetricNameEncoded("Consumer.Topic.Latency", item.topic());
            if (!m_metrics.getNames().contains(new Gson().toJson(consumerTopicLatency)))
            {
                if (appProperties.sendConsumerTopicLatency)
                    m_metrics.register(new Gson().toJson(consumerTopicLatency), histogramConsumerTopicLatency);
            }

            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {
                MetricNameEncoded consumerPartitionLatency = new MetricNameEncoded("Consumer.Partition.Latency", item.topic() + part.partitionId());
                Histogram histogramConsumerPartitionLatency = new Histogram(new SlidingWindowReservoir(1));
                if (!m_metrics.getNames().contains(new Gson().toJson(consumerPartitionLatency)))
                {
                    if (appProperties.sendConsumerPartitionLatency)
                        m_metrics.register(new Gson().toJson(consumerPartitionLatency), histogramConsumerPartitionLatency);
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
                    m_logger.error("Error Reading from Topic: {}; Partition: {}; Exception: {}", item.topic(), part.partitionId(), e);
                    consumerFailCount++;
                }
            }
        }
        if (appProperties.sendConsumerAvailability)
        {
            MetricNameEncoded consumerAvailability = new MetricNameEncoded("Consumer.Availability", "all");
            m_metrics.register(new Gson().toJson(consumerAvailability), new AvailabilityGauge(consumerTryCount, consumerTryCount - consumerFailCount));
        }
        ((MetaDataManager)metaDataManager).close();
    }
}
