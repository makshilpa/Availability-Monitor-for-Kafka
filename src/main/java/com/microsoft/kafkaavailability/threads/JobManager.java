//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.threads;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

import static com.microsoft.kafkaavailability.discovery.Constants.DEFAULT_ELAPSED_TIME;

public class JobManager implements Callable<Long> {
    final static Logger m_logger = LoggerFactory.getLogger(JobManager.class);
    protected long timeout;
    protected TimeUnit timeUnit;
    protected Callable<Long> job;

    public JobManager(long timeout, TimeUnit timeUnit, Callable<Long> job) {
        this.timeout = timeout;
        this.timeUnit = timeUnit;
        this.job = job;
    }

    @Override
    public Long call() {
        Long elapsedTime = new Long(DEFAULT_ELAPSED_TIME);
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            elapsedTime = executorService.submit(job).get(timeout, timeUnit);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            if (e instanceof TimeoutException) {
                m_logger.error("Thread Timeout of " + timeout + " " + timeUnit + " occurred for " + job.toString());
            } else {
                m_logger.error("Exception occurred for " + job.toString() + " : " + e.getMessage());
            }
        }
        executorService.shutdown();
        return elapsedTime;
    }
}