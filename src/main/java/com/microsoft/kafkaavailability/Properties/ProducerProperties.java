//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */

package com.microsoft.kafkaavailability.properties;

public class ProducerProperties
{
    public String serializer_class;
    public String partitioner_class;
    public Integer request_required_acks;
    public String messageStart;
}
