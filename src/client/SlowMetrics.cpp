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

#include "SlowMetrics.h"

namespace Hdfs {
namespace Internal {

SlowMetric::SlowMetric(long _value) {
    value = _value;
    milliTime = steady_clock::now();
}

long SlowMetric::getValue() {
    return value;
}

steady_clock::time_point SlowMetric::getMilliTime() {
    return milliTime;
}

SlowMetrics::SlowMetrics(long _threshold, long _count, long _intervals) {
    threshold = _threshold;
    count = _count;
    intervalMs = _intervals * 1000;
}

bool SlowMetrics::putMetric(long value) {
    lock_guard<mutex> lock(mut);
    if (slowCounts() > 0) {
        // delete metric beyond the interval.
        deleteTimeoutMetric();
    }
    if (value >= threshold) {
        std::shared_ptr<SlowMetric> metric = std::shared_ptr<SlowMetric>(new SlowMetric(value));
        slowMetricList.push_back(metric);
        return true;
    }
    return false;
}

void SlowMetrics::deleteTimeoutMetric() {
    if (slowMetricList.empty()) {
        return;
    }
    std::shared_ptr<SlowMetric> metric = slowMetricList.front();
    steady_clock::time_point now = steady_clock::now();
    while (slowMetricList.size() > 0 &&
        ToMilliSeconds(metric->getMilliTime(), steady_clock::now()) > intervalMs) {
        // delete the metric beyond the interval.
        slowMetricList.pop_front();
        if (slowMetricList.size() > 0) {
            metric = slowMetricList.front();
        }
    }
}

bool SlowMetrics::inSlowState() {
    lock_guard<mutex> lock(mut);
    if (slowCounts() > 0) {
        // delete metric beyond the interval.
        deleteTimeoutMetric();
    }
    return slowMetricList.size() >= count;
}

long SlowMetrics::slowCounts() {
    return slowMetricList.size();
}

double SlowMetrics::getAvg() {
    lock_guard<mutex> lock(mut);
    if (slowMetricList.size() > 0) {
        double sum = 0;
        for (std::shared_ptr<SlowMetric> metric : slowMetricList) {
            sum += metric->getValue();
        }
        return sum / slowMetricList.size();
    } else {
        return 0;
    }
}

void SlowMetrics::clear() {
    slowMetricList.clear();
}

}
}