/********************************************************************
 * 2023 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIBHDFS3_SLOWMETRICS_H
#define LIBHDFS3_SLOWMETRICS_H

#include <chrono>
#include <deque>

#include "Thread.h"
#include "DateTime.h"

namespace Hdfs {
namespace Internal {

class SlowMetric {
public:
    SlowMetric(long value);

    long getValue();

    steady_clock::time_point getMilliTime();

private:
    steady_clock::time_point milliTime;
    long value;
};

class SlowMetrics {

public:
    SlowMetrics(long _threshold, long _count, long _intervalNs);

    bool putMetric(long value);

    bool inSlowState();

    double getAvg();

    void clear();

private:
    void deleteTimeoutMetric();

    long slowCounts();

private:
    /* the threshold for slow metric */
    long threshold;
    /* the count of slow metrics in the interval */
    long count;
    long intervalMs;
    std::deque<std::shared_ptr<SlowMetric>> slowMetricList;
    mutex mut;
};

}
}

#endif //LIBHDFS3_SLOWMETRICS_H
