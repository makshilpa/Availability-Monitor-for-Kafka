//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package Microsoft.KafkaAvailability;

public class TopicPartition
{
    public String topic;
    public int partitionId;
    TopicPartition(String topic, int partitionId)
    {
        this.topic = topic;
        this.partitionId = partitionId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(obj instanceof TopicPartition)
        {
            TopicPartition otherObj = (TopicPartition) obj;
            if (topic.equals(otherObj.topic) && partitionId == otherObj.partitionId)
                return true;
        }
        return false;
    }
    @Override
    public int hashCode() {
        return new String(topic + partitionId).hashCode();
    }
}
