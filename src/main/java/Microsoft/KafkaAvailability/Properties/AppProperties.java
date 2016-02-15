//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************
/**
 * Created by Akshat Kaul
 */
package Microsoft.KafkaAvailability.Properties;

public class AppProperties
{
    public String sqlConnectionString;
    public boolean reportToSql;
    public boolean reportToSlf4j;
    public boolean reportToConsole;
    public boolean reportToCsv;
    public boolean reportToJmx;
    public String csvDirectory;
}
