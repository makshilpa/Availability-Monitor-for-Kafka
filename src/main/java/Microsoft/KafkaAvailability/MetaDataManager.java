//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package Microsoft.KafkaAvailability;

import Microsoft.KafkaAvailability.Properties.MetaDataManagerProperties;
import com.google.common.reflect.Parameter;
import com.google.gson.Gson;
import org.apache.zookeeper.KeeperException;
import scala.Option;
import scala.collection.JavaConversions;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/***
 * Gets the list of Kafka brokers.
 * Gets Topic and Partition metadata.
 */
public class MetaDataManager implements IMetaDataManager
{
    MetaDataManagerProperties m_mDProps;
    List<String> m_brokerIds;

    public MetaDataManager(IPropertiesManager<MetaDataManagerProperties> propManager)
    {
        m_mDProps = propManager.getProperties();

    }

    /***
     * Gets list of broker IPs from the properties file in the form 1.1.1.1:2181,2.2.2.2:2181
     *
     * @param addPort If true, adds port to the broker ip
     * @return List of broker IPs
     */
    @Override
    public List<String> getBrokerList(boolean addPort) throws MetaDataManagerException
    {
        if (m_mDProps.useZooKeeper)
        {
            m_brokerIds = getBrokerListFromZooKeeper(addPort);
        } else
        {
            m_brokerIds = m_mDProps.brokerList;
        }
        return m_brokerIds;
    }

    /***
     * Gets list of broker IPs from zookeeper in the form 1.1.1.1:2181,2.2.2.2:2181
     *
     * @param addPort If true, adds port to the broker ip
     * @return List of broker IPs
     */
    private List<String> getBrokerListFromZooKeeper(boolean addPort) throws MetaDataManagerException
    {
        ZooKeeper zooKeeper;
        List<String> brokerInfos = new ArrayList<String>();
        Gson gson = new Gson();
        int exceptionCount = 0;
        try
        {
            System.out.println(m_mDProps.zooKeeperHosts + " " + m_mDProps.soTimeout);
            zooKeeper = new ZooKeeper(m_mDProps.zooKeeperHosts, m_mDProps.soTimeout, null);
            List<String> ids = zooKeeper.getChildren("/brokers/ids", false);

            for (String id : ids)
            {
                String brokerInfoStr = new String(zooKeeper.getData("/brokers/ids/" + id, false, null));
                BrokerInfo brokerInfo = gson.fromJson(brokerInfoStr, BrokerInfo.class);
                if (addPort)
                {
                    brokerInfos.add(brokerInfo.host + ":" + brokerInfo.port);
                } else
                {
                    brokerInfos.add(brokerInfo.host);
                }
            }
            zooKeeper.close();
        } catch (ArrayIndexOutOfBoundsException e)
        {
            exceptionCount++;
            if (exceptionCount < m_mDProps.acceptable_exception_count)
            {
                System.out.println(e.toString());
            } else
            {
                throw new MetaDataManagerException(e.getMessage());
            }
        } catch (IOException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        } catch(InterruptedException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        }catch (KeeperException e)
        {
            throw new MetaDataManagerException(e.getMessage());
        }


        if (brokerInfos.isEmpty())
        {
            System.out.println("Could not connect to ZooKeeper");
        }
        return brokerInfos;
    }

    /***
     * Gets metadata for all topics in the cluster
     *
     * @return List topic metadata
     */
    @Override
    public List<kafka.javaapi.TopicMetadata> getMetaDataFromAllBrokers()
    {
        List<String> topics = new ArrayList<String>();
        if (m_mDProps.useWhiteList)
        {
            topics.addAll(m_mDProps.topicsWhitelist);
        }
        TopicMetadataRequest req = new TopicMetadataRequest(topics);
        List<kafka.javaapi.TopicMetadata> allMetaData = new ArrayList<kafka.javaapi.TopicMetadata>();
        for (String brokerId : m_brokerIds)
        {
            SimpleConsumer consumer = new SimpleConsumer(
                    brokerId.split(":")[0],
                    Integer.parseInt(brokerId.split(":")[1]),
                    m_mDProps.soTimeout,
                    m_mDProps.bufferSize,
                    m_mDProps.clientId);
            kafka.javaapi.TopicMetadataResponse resp = null;
            try
            {
                resp = consumer.send(req);
                allMetaData.addAll(resp.topicsMetadata());
            } catch (Exception e)
            {
                System.out.println(e);
            }
        }
        return allMetaData;
    }

    /***
     * Dedupe the partition metadata from all brokers
     *
     * @return Deduped topic metadata
     */
    public List<kafka.javaapi.TopicMetadata> getAllTopicPartition()
    {
        List<kafka.javaapi.TopicMetadata> data = getMetaDataFromAllBrokers();
        HashSet<TopicPartition> exploredTopicPartition = new HashSet<TopicPartition>();
        List<kafka.javaapi.TopicMetadata> ret = new ArrayList<TopicMetadata>();
        for (TopicMetadata item : data)
        {

            List<kafka.api.PartitionMetadata> pml = new ArrayList<kafka.api.PartitionMetadata>();
            for (PartitionMetadata part : item.partitionsMetadata())
            {
                if (!exploredTopicPartition.contains(new TopicPartition(item.topic(), part.partitionId())))
                {
                    kafka.api.PartitionMetadata pm =
                            new kafka.api.PartitionMetadata(
                                    part.partitionId(),
                                            Option.apply(part.leader()),
                                            JavaConversions.asScalaBuffer(part.replicas()).toList(),
                                            JavaConversions.asScalaBuffer(part.isr()).toList(),
                                            part.errorCode());
                    pml.add(pm);
                    exploredTopicPartition.add(new TopicPartition(item.topic(), part.partitionId()));
                }

            }
            if (pml.size() > 0)
            {
                kafka.api.TopicMetadata tm =
                        new kafka.api.TopicMetadata(
                                item.topic(),
                                JavaConversions.asScalaBuffer(pml).toList(),
                                item.errorCode());
                ret.add(new kafka.javaapi.TopicMetadata(tm));
            }
        }
        return ret;
    }

    /***
     * Print all the metadata
     */
    @Override
    public void printEverything()
    {
        List<kafka.javaapi.TopicMetadata> data = getAllTopicPartition();
        for (kafka.javaapi.TopicMetadata item : data)
        {
            System.out.println("Topic: " + item.topic());
            for (kafka.javaapi.PartitionMetadata part : item.partitionsMetadata())
            {
                String replicas = "";
                String isr = "";
                for (kafka.cluster.Broker replica : part.replicas())
                {
                    replicas += " " + replica.host();
                }
                for (kafka.cluster.Broker replica : part.isr())
                {
                    isr += " " + replica.host();
                }
                String leader = "";
                if (part.leader() != null)
                {
                    if (part.leader().host() != null)
                        leader = part.leader().host();
                }
                System.out.println("    Partition: " + part.partitionId() + ": Leader: " + leader + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
            }
        }
    }
}