//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import com.codahale.metrics.*;
import com.codahale.metrics.Timer;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A reporter which sends the measurements for each metric to a SQL Database.
 */
public class SqlReporter extends ScheduledReporter
{
    /**
     *
     * @param registry the registry to report
     * @return a {@link Builder}
     */
    public static Builder forRegistry(MetricRegistry registry)
    {
        return new Builder(registry);
    }

    /***
     *
     * @param gauges map of gauge names and objects
     * @param counters map of counter names and objects
     * @param histograms map of histogram names and objects
     * @param meters map with of meter and objects
     * @param timers map with of timer and objects
     */
    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers)
    {
        final long timestamp = TimeUnit.MILLISECONDS.toSeconds(clock.getTime());

        for (Map.Entry<String, Gauge> entry : gauges.entrySet())
        {
            reportGauge(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Counter> entry : counters.entrySet())
        {
            reportCounter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Histogram> entry : histograms.entrySet())
        {
            reportHistogram(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Meter> entry : meters.entrySet())
        {
            reportMeter(timestamp, entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Timer> entry : timers.entrySet())
        {
            reportTimer(timestamp, entry.getKey(), entry.getValue());
        }
    }

    /**
     * A builder for  instances. Defaults to using the default locale, converting
     * rates to events/second, converting durations to milliseconds, and not filtering metrics.
     */
    public static class Builder
    {
        private final MetricRegistry registry;
        private Locale locale;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private Clock clock;
        private MetricFilter filter;

        private Builder(MetricRegistry registry)
        {
            this.registry = registry;
            this.locale = Locale.getDefault();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.clock = Clock.defaultClock();
            this.filter = MetricFilter.ALL;
        }

        /**
         * Format numbers for the given {@link Locale}.
         *
         * @param locale a {@link Locale}
         * @return {@code this}
         */
        public Builder formatFor(Locale locale)
        {
            this.locale = locale;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit)
        {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit)
        {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock)
        {
            this.clock = clock;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter)
        {
            this.filter = filter;
            return this;
        }


        /**
         *
         *
         * @param connectionString
         * @return a {@link SqlReporter}
         */
        /***
         * Builds a {@link SqlReporter} with the given properties
         * @param connectionString the connectionString for the db
         * @param userId typically the cluster name where the service is running
         * @return {@link SqlReporter}
         */
        public SqlReporter build(String connectionString, String userId)
        {
            return new SqlReporter(registry,
                    connectionString,
                    userId,
                    locale,
                    rateUnit,
                    durationUnit,
                    clock,
                    filter);
        }
    }

    private static final Logger m_logger = LoggerFactory.getLogger(SqlReporter.class);
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    private final String connectionString;
    private final String userId;

    private final Locale locale;
    private final Clock clock;

    private SqlReporter(MetricRegistry registry,
                        String connectionString,
                        String userId,
                        Locale locale,
                        TimeUnit rateUnit,
                        TimeUnit durationUnit,
                        Clock clock,
                        MetricFilter filter)
    {
        super(registry, "sql-reporter", filter, rateUnit, durationUnit);
        this.connectionString = connectionString;
        this.userId = userId;
        this.locale = locale;
        this.clock = clock;
    }


    private void reportTimer(long timestamp, String name, Timer timer)
    {
        final Snapshot snapshot = timer.getSnapshot();

        report(timestamp,
                name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit,duration_unit",
                "'%d','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','%f','calls/%s','%s'",
                timer.getCount(),
                convertDuration(snapshot.getMax()),
                convertDuration(snapshot.getMean()),
                convertDuration(snapshot.getMin()),
                convertDuration(snapshot.getStdDev()),
                convertDuration(snapshot.getMedian()),
                convertDuration(snapshot.get75thPercentile()),
                convertDuration(snapshot.get95thPercentile()),
                convertDuration(snapshot.get98thPercentile()),
                convertDuration(snapshot.get99thPercentile()),
                convertDuration(snapshot.get999thPercentile()),
                convertRate(timer.getMeanRate()),
                convertRate(timer.getOneMinuteRate()),
                convertRate(timer.getFiveMinuteRate()),
                convertRate(timer.getFifteenMinuteRate()),
                getRateUnit(),
                getDurationUnit());
    }

    private void reportMeter(long timestamp, String name, Meter meter)
    {
        report(timestamp,
                name,
                "count,mean_rate,m1_rate,m5_rate,m15_rate,rate_unit",
                "'%d','%f','%f','%f','%f','events/%s'",
                meter.getCount(),
                convertRate(meter.getMeanRate()),
                convertRate(meter.getOneMinuteRate()),
                convertRate(meter.getFiveMinuteRate()),
                convertRate(meter.getFifteenMinuteRate()),
                getRateUnit());
    }

    private void reportHistogram(long timestamp, String name, Histogram histogram)
    {
        final Snapshot snapshot = histogram.getSnapshot();

        report(timestamp,
                name,
                "count,max,mean,min,stddev,p50,p75,p95,p98,p99,p999",
                "'%d','%d','%f','%d','%f','%f','%f','%f','%f','%f','%f'",
                histogram.getCount(),
                snapshot.getMax(),
                snapshot.getMean(),
                snapshot.getMin(),
                snapshot.getStdDev(),
                snapshot.getMedian(),
                snapshot.get75thPercentile(),
                snapshot.get95thPercentile(),
                snapshot.get98thPercentile(),
                snapshot.get99thPercentile(),
                snapshot.get999thPercentile());
    }

    private void reportCounter(long timestamp, String name, Counter counter)
    {
        report(timestamp, name, "count", "'%d'", counter.getCount());
    }

    private void reportGauge(long timestamp, String name, Gauge gauge)
    {
        report(timestamp, name, "value", "'%s'", gauge.getValue());
    }

    private void report(long timestamp, String name, String header, String line, Object... values) {
        Connection con = null;
        Statement stmt = null;
        int iMaxRetries = 10;
        int iRetryInterval = 5;
        boolean retry = true;

        MetricNameEncoded metricNameEncoded = new Gson().fromJson(name, MetricNameEncoded.class);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        try {
            for (int iRetryCount = 0; iRetryCount < iMaxRetries && retry; iRetryCount++) {
                try {
                    con = getConnection(connectionString);

                    // Create and execute an SQL statement that returns some data.
                    String SQL = String.format(String.format("insert into [dbo].[%s] values('%s','%s','%s',%s)", metricNameEncoded.name, userId, sdf.format(new Date(timestamp * 1000)), metricNameEncoded.tag, line), values);

                    if (null != con) {
                        stmt = con.createStatement();
                        if (null != stmt) {
                            stmt.execute(SQL);
                            //Setting retry to false to exit the loop
                            retry = false;
                        }
                    }
                } catch (java.sql.SQLException e) {
                    logSQLException(e);
                } finally {
                    // Clean up the command and Database Connection objects
                    try {
                        if (stmt != null) stmt.close();
                    } catch (java.sql.SQLException e) {
                        logSQLException(e);
                    }

                    // Clean up Connection object
                    try {
                        if (con != null) con.close();
                    } catch (java.sql.SQLException e) {
                        logSQLException(e);
                    }
                    con = null;
                }
                // If the maximum number of retries not exceeded
                if (retry && iRetryCount < iMaxRetries) {
                    // Sleep and continue (convert to milliseconds)
                    Thread.sleep(iRetryInterval * 1000);
                }
            }
        }
        // unreported exception java.lang.InterruptedException; must be caught or declared to be thrown
        catch (Exception ex) {
            ex.printStackTrace();
            m_logger.error(ex.getMessage(), ex);
        }
    }

    /**
     Return the connection instance to the SQL Server.
     Load the SQLServerDriver class, build the connection string
     **/
    public Connection getConnection(String connStr) {

        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            Connection conn = java.sql.DriverManager.getConnection(connStr);
            return conn;
        } catch (Exception ex) {
            ex.printStackTrace();
            m_logger.error(ex.getMessage(), ex);
        }
        return null;
    }

    // Display an SQLException which has occured in this application.
    private static void logSQLException (java.sql.SQLException e) {
        // Notice that a SQLException is actually a chain of SQLExceptions,
        // let's print all of them...
        java.sql.SQLException next = e;
        while (next != null) {
            m_logger.error(next.getMessage());
            m_logger.error("Error Code: " + next.getErrorCode());
            m_logger.error("SQL State: " + next.getSQLState());
            next = next.getNextException();
        }
    }
}
