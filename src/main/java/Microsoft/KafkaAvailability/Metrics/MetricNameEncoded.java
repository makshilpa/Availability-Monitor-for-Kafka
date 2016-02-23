//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package Microsoft.KafkaAvailability.Metrics;

public class MetricNameEncoded
{
    public String name;
    public String tag;
    public MetricNameEncoded(String name, String tag)
    {
        this.name = name;
        this.tag = tag;
    }
}

