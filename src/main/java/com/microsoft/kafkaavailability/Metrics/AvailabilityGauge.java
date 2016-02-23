//*********************************************************
// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
//*********************************************************

package com.microsoft.kafkaavailability.metrics;

import com.codahale.metrics.RatioGauge;

public class AvailabilityGauge extends RatioGauge
{
        private final int tries;
        private final int successes;

        public AvailabilityGauge(int tries, int successes) {
            this.tries = tries;
            this.successes = successes;
        }

        @Override
        public Ratio getRatio() {
            if(tries == 0)
                return Ratio.of(0,1);
            return Ratio.of(successes,tries);
        }
}
