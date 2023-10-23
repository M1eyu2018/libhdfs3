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

#include "SlowMetricsMap.h"

namespace Hdfs {
namespace Internal {

std::string SlowMetricsMap::buildKey(const DatanodeInfo & datanode) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << datanode.getIpAddr() << datanode.getXferPort()
       << datanode.getDatanodeId();
    return ss.str();
}

std::shared_ptr<SlowMetrics> SlowMetricsMap::get(const DatanodeInfo & datanode) {
    lock_guard<mutex> lock(mut);
    std::string key = buildKey(datanode);
    if (innerMap.find(key) != innerMap.end()) {
        return innerMap[key];
    } else {
        std::shared_ptr<SlowMetrics> slowMetrics;
        return slowMetrics;
    }
}

void SlowMetricsMap::put(const DatanodeInfo & datanode, std::shared_ptr<SlowMetrics> slowMetrics) {
    lock_guard<mutex> lock(mut);
    std::string key = buildKey(datanode);
    innerMap[key] = slowMetrics;
}

}
}
