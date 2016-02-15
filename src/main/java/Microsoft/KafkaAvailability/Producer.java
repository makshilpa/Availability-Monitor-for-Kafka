//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package Microsoft.KafkaAvailability;

import Microsoft.KafkaAvailability.Properties.ProducerProperties;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/***
 * Responsible for sending canary messages to specified topics and partitions in Kafka
 */
public class Producer implements IProducer
{
    private IPropertiesManager<ProducerProperties> m_propManager;
    private IMetaDataManager m_metaDataManager;
    private ProducerProperties producerProperties;
    private kafka.javaapi.producer.Producer<String, String> m_producer;

    /***
     *
     * @param propManager Used to get properties from json file
     * @param metaDataManager Used to get the broker list
     */
    public Producer(IPropertiesManager<ProducerProperties> propManager, IMetaDataManager metaDataManager) throws MetaDataManagerException
    {
        m_metaDataManager = metaDataManager;
        m_propManager = propManager;
        producerProperties = propManager.getProperties();
        Properties props = new Properties();
        String brokerList = "";
        for (String broker : m_metaDataManager.getBrokerList(true))
        {
            brokerList += broker + ", ";
        }
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", producerProperties.serializer_class);
        props.put("partitioner.class", SimplePartitioner.class.getName());
        props.put("request.required.acks", producerProperties.request_required_acks.toString());

        ProducerConfig config = new ProducerConfig(props);
        m_producer = new kafka.javaapi.producer.Producer<String, String>(config);
    }

    /***
     * Sends the message to specified topic and partition
     * @param topicName topic name
     * @param partitionId partition id
     */
    @Override
    public void SendCanaryToTopicPartition(String topicName, String partitionId)
    {
        m_producer.send(createCanaryMessage(topicName, partitionId));
    }

    /***
     * Constructs the canary message to be sent.
     * The message is encoded with the topic and partition information to tell Kafka where it should land.
     * @param topicName topic name
     * @param partitionId partition id
     * @return
     */
    private KeyedMessage<String, String> createCanaryMessage(String topicName, String partitionId)
    {
        long runtime = new Date().getTime();
        String msg = producerProperties.messageStart + runtime + ",www.example.com," + partitionId;
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topicName, partitionId, msg);
        return data;
    }
}
