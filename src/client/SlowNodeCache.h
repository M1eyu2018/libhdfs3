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

#ifndef LIBHDFS3_SLOWNODECACHE_H
#define LIBHDFS3_SLOWNODECACHE_H

#include "Thread.h"
#include "common/DateTime.h"
#include "common/LruMap.h"
#include "common/SessionConfig.h"
#include "server/DatanodeInfo.h"


namespace Hdfs {
namespace Internal {

class SlowNodeCache {
public:
    explicit SlowNodeCache(const SessionConfig & conf);

    bool containsKey(const DatanodeInfo & datanode);

    void put(const DatanodeInfo & datanode);

private:
    std::string buildKey(const DatanodeInfo & datanode);

private:
    const int cacheSize;
    long expireTimeInterval;  // milliseconds
    static LruMap<std::string, steady_clock::time_point> Map;
};

}
}

#endif //LIBHDFS3_SLOWNODECACHE_H
