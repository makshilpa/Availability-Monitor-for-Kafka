//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingWindowReservoir;
import com.google.gson.Gson;
import com.microsoft.kafkaavailability.*;
import com.microsoft.kafkaavailability.discovery.CommonUtils;
import com.microsoft.kafkaavailability.metrics.AvailabilityGauge;
import com.microsoft.kafkaavailability.metrics.MetricNameEncoded;
import com.microsoft.kafkaavailability.properties.AppProperties;
import com.microsoft.kafkaavailability.properties.MetaDataManagerProperties;
import com.microsoft.kafkaavailability.properties.ProducerProperties;
import kafka.javaapi.TopicMetadata;
import org.apache.curator.framework.CuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;

public class AvailabilityThread implements Runnable {

    final static Logger m_logger = LoggerFactory.getLogger(AvailabilityThread.class);
    Phaser m_phaser;
    CuratorFramework m_curatorFramework;
    MetricRegistry m_metrics;
    int m_threadSleepTime;

    public AvailabilityThread(Phaser phaser, CuratorFramework curatorFramework, MetricRegistry metrics, int threadSleepTime) {
        this.m_phaser = phaser;
        this.m_curatorFramework = curatorFramework;
        //this.m_phaser.register(); //Registers/Add a new unArrived party to this phaser.
        //CommonUtils.dumpPhaserState("After register", phaser);
        m_metrics = metrics;
        m_threadSleepTime = threadSleepTime;
    }

    @Override
    public void run() {
        int sleepDuration = 1000;
        do {
            m_logger.info(Thread.currentThread().getName() +
                    " - Availability party has arrived and is working in "
                    + "Phase-" + m_phaser.getPhase());

            long lStartTime = System.nanoTime();

            try {
                RunAvailability();
            } catch (Exception e) {
                m_logger.error(e.getMessage(), e);
            }
            long elapsedTime = CommonUtils.stopWatch(lStartTime);
            m_logger.info("Availability Elapsed: " + elapsedTime + " milliseconds.");

            while (elapsedTime < m_threadSleepTime && !m_phaser.isTerminated()) {
                try {
                    Thread.currentThread().sleep(sleepDuration);
                    elapsedTime = elapsedTime + sleepDuration;
                } catch (InterruptedException ie) {
                    m_logger.error(ie.getMessage(), ie);
                }
            }
            //phaser.arriveAndAwaitAdvance();
        } while (!m_phaser.isTerminated());
        m_logger.info("AvailabilityThread (run()) has been COMPLETED.");
    }

    private void RunAvailability() throws IOException, MetaDataManagerException {

        m_logger.info("Starting AvailabilityLatency");

        IPropertiesManager producerPropertiesManager = new PropertiesManager<ProducerProperties>("producerProperties.json", ProducerProperties.class);
        IPropertiesManager metaDataPropertiesManager = new PropertiesManager<MetaDataManagerProperties>("metadatamanagerProperties.json", MetaDataManagerProperties.class);
        IMetaDataManager metaDataManager = new MetaDataManager(m_curatorFramework, metaDataPropertiesManager);
        MetaDataManagerProperties metaDataProperties = (MetaDataManagerProperties) metaDataPropertiesManager.getProperties();

        IProducer producer = new Producer(producerPropertiesManager, metaDataManager);

        IPropertiesManager appPropertiesManager = new PropertiesManager<AppProperties>("appProperties.json", AppProperties.class);
        AppProperties appProperties = (AppProperties) appPropertiesManager.getProperties();

        int clusterIPStatusTryCount = 0, gtmIPStatusTryCount = 0;
        int clusterIPStatusFailCount = 0, gtmIPStatusFailCount = 0;
        long startTime, endTime;
        int numMessages = 100;
        int windowSize = 0;

        //This is full list of topics
        List<TopicMetadata> totalTopicMetadata = metaDataManager.getAllTopicPartition();

        List<kafka.javaapi.TopicMetadata> whiteListTopicMetadata = new ArrayList<TopicMetadata>();

        for (kafka.javaapi.TopicMetadata topic : totalTopicMetadata) {
            for (String whiteListTopic : metaDataProperties.topicsWhitelist)
                // java string compare while ignoring case
                if (topic.topic().equalsIgnoreCase(whiteListTopic)) {
                    whiteListTopicMetadata.add(topic);
                }
        }

        windowSize = numMessages * ((whiteListTopicMetadata.size() > 0) ? whiteListTopicMetadata.size() : 1);

        final SlidingWindowReservoir gtmAvailabilityLatencyWindow = new SlidingWindowReservoir(windowSize);
        Histogram histogramGTMAvailabilityLatency = new Histogram(gtmAvailabilityLatencyWindow);
        MetricNameEncoded gtmAvailabilityLatency = new MetricNameEncoded("KafkaGTMIP.Availability.Latency", "all");
        if (!m_metrics.getNames().contains(new Gson().toJson(gtmAvailabilityLatency))) {
            if (appProperties.sendGTMAvailabilityLatency)
                m_metrics.register(new Gson().toJson(gtmAvailabilityLatency), histogramGTMAvailabilityLatency);
        }

        final SlidingWindowReservoir IPAvailabilityLatencyWindow = new SlidingWindowReservoir(windowSize);
        Histogram histogramIPAvailabilityLatency = new Histogram(IPAvailabilityLatencyWindow);
        MetricNameEncoded ipAvailabilityLatency = new MetricNameEncoded("KafkaIP.Availability.Latency", "all");
        if (!m_metrics.getNames().contains(new Gson().toJson(ipAvailabilityLatency))) {
            if (appProperties.sendIPAvailabilityLatency)
                m_metrics.register(new Gson().toJson(ipAvailabilityLatency), histogramIPAvailabilityLatency);
        }

        m_logger.info("Starting KafkaIP prop check." + appProperties.reportKafkaIPAvailability);
        m_logger.info("Starting KafkaGTM (VIP) prop check." + appProperties.reportKafkaGTMAvailability);
        for (int i = 0; i < windowSize; i++) {
            for (kafka.javaapi.TopicMetadata item : whiteListTopicMetadata) {
                if (appProperties.reportKafkaIPAvailability) {
                    startTime = System.currentTimeMillis();
                    try {
                        clusterIPStatusTryCount++;
                        producer.SendCanaryToKafkaIP(appProperties.kafkaClusterIP, item.topic(), false);
                        endTime = System.currentTimeMillis();
                    } catch (Exception e) {
                        clusterIPStatusFailCount++;
                        m_logger.error("ClusterIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                        endTime = System.currentTimeMillis() + 60000;
                    }

                    histogramIPAvailabilityLatency.update(endTime - startTime);
                }
                if (appProperties.reportKafkaGTMAvailability) {
                    startTime = System.currentTimeMillis();
                    try {
                        gtmIPStatusTryCount++;
                        producer.SendCanaryToKafkaIP(appProperties.kafkaGTMIP, item.topic(), false);
                        endTime = System.currentTimeMillis();
                    } catch (Exception e) {
                        gtmIPStatusFailCount++;
                        m_logger.error("GTMIPStatus -- Error Writing to Topic: {}; Exception: {}", item.topic(), e);
                        endTime = System.currentTimeMillis() + 60000;
                    }
                    histogramGTMAvailabilityLatency.update(endTime - startTime);
                }
            }

        }
        m_logger.info("done with VIP prop check.");
        if (appProperties.reportKafkaIPAvailability) {
            m_logger.info("About to report kafkaClusterIPAvailability-- TryCount:" + clusterIPStatusTryCount + " FailCount:" + clusterIPStatusFailCount);
            MetricNameEncoded kafkaClusterIPAvailability = new MetricNameEncoded("KafkaIP.Availability", "all");
            if (!m_metrics.getNames().contains(new Gson().toJson(kafkaClusterIPAvailability))) {
                m_metrics.register(new Gson().toJson(kafkaClusterIPAvailability), new AvailabilityGauge(clusterIPStatusTryCount, clusterIPStatusTryCount - clusterIPStatusFailCount));
            }
        }
        if (appProperties.reportKafkaGTMAvailability) {
            m_logger.info("About to report kafkaGTMIPAvailability-- TryCount:" + gtmIPStatusTryCount + " FailCount:" + gtmIPStatusFailCount);
            MetricNameEncoded kafkaGTMIPAvailability = new MetricNameEncoded("KafkaGTMIP.Availability", "all");
            if (!m_metrics.getNames().contains(new Gson().toJson(kafkaGTMIPAvailability))) {
                m_metrics.register(new Gson().toJson(kafkaGTMIPAvailability), new AvailabilityGauge(gtmIPStatusTryCount, gtmIPStatusTryCount - gtmIPStatusFailCount));
            }
        }

        ((MetaDataManager) metaDataManager).close();
        m_logger.info("Finished AvailabilityLatency");
    }
}