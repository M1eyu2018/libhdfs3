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

#include <inttypes.h>

#include "SlowNodeCache.h"

namespace Hdfs {
namespace Internal {

LruMap<std::string, steady_clock::time_point> SlowNodeCache::Map;

SlowNodeCache::SlowNodeCache(const SessionConfig & conf)
    : cacheSize(conf.getSlowNodeCacheCapacity()), expireTimeInterval(conf.getSlowNodeCacheExpiry()) {
    Map.setMaxSize(cacheSize);
}

std::string SlowNodeCache::buildKey(const DatanodeInfo & datanode) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << datanode.getIpAddr() << datanode.getXferPort()
       << datanode.getDatanodeId();
    return ss.str();
}

bool SlowNodeCache::containsKey(const DatanodeInfo & datanode) {
    std::string key = buildKey(datanode);
    steady_clock::time_point value;
    int64_t elipsed;

    if (Map.find(key, &value)) {
        if ((elipsed = ToMilliSeconds(value, steady_clock::now())) > expireTimeInterval) {
            LOG(DEBUG1, "SlowNodeCache expire for datanode %s uuid(%s).",
                datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
            Map.erase(key);
            return false;
        } else {
            LOG(DEBUG1, "SlowNodeCache hit for datanode %s uuid(%s), elipsed %" PRId64,
                datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str(),
                elipsed);
            return true;
        }
    }

    LOG(DEBUG1, "SlowNodeCache miss for datanode %s uuid(%s).",
        datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
    return false;
}

void SlowNodeCache::put(const DatanodeInfo & datanode) {
    std::string key = buildKey(datanode);
    steady_clock::time_point value = steady_clock::now();
    Map.insert(key, value);
    LOG(DEBUG1, "SlowNodeCache add for datanode %s uuid(%s).",
        datanode.formatAddress().c_str(), datanode.getDatanodeId().c_str());
}

}
}