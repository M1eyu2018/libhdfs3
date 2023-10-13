/********************************************************************
 * Copyright (c) 2013 - 2014, Pivotal Inc.
 * All rights reserved.
 *
 * Author: Zhanwei Wang
 ********************************************************************/
/********************************************************************
 * 2014 -
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
#ifndef _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_
#define _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_

#include <string>

#include "datatransfer.pb.h"
#include "LongBitFormat.h"

namespace Hdfs {
namespace Internal {

enum class SLOW {
    NORMAL, SLOW
};

class StatusFormat {
public:
    static int setSLOW(int old, SLOW slow) {
        return (int) SLOW_BITS->BITS->combine(static_cast<long>(slow), old);
    }

    static SLOW getSLOW(int header) {
        return SLOW((int) SLOW_BITS->BITS->retrieve(header));
    }

private:
    StatusFormat(std::string name, int offset, int bits) {
        BITS = std::shared_ptr<LongBitFormat>(new LongBitFormat(name, offset, bits, 0));
    }

private:
    static const int STATUS_LENGTH = 4;
    static const int RESERVED_LENGTH = 1;
    static const int ECN_BITS_LENGTH = 2;
    static const int SLOW_BITS_LENGTH = 1;
    // offset can't be greater than 64 bits
    static const int STATUS_OFFSET = 0;
    static const int RESERVED_OFFSET = STATUS_LENGTH;
    static const int ECN_BITS_OFFSET = STATUS_LENGTH + RESERVED_LENGTH;
    static const int SLOW_BITS_OFFSET = STATUS_LENGTH + RESERVED_LENGTH + ECN_BITS_OFFSET;
    std::shared_ptr<LongBitFormat> BITS;
    static std::shared_ptr<StatusFormat> STATUS;
    static std::shared_ptr<StatusFormat> RESERVED;
    static std::shared_ptr<StatusFormat> ECN_BITS;
    static std::shared_ptr<StatusFormat> SLOW_BITS;
};

class PipelineAck {
public:
    PipelineAck() :
        invalid(true) {
    }

    PipelineAck(const char * buf, int size) :
        invalid(false) {
        readFrom(buf, size);
    }

    bool isInvalid() {
        return invalid;
    }

    int getNumOfReplies() {
        return proto.status_size();
    }

    int64_t getSeqno() {
        return proto.seqno();
    }

    Status getReply(int i) {
        return proto.status(i);
    }

    bool isSuccess() {
        int size = proto.status_size();

        for (int i = 0; i < size; ++i) {
            if (Status::DT_PROTO_SUCCESS != proto.status(i)) {
                return false;
            }
        }

        return true;
    }

    void readFrom(const char * buf, int size) {
        invalid = !proto.ParseFromArray(buf, size);
    }

    void reset() {
        proto.Clear();
        invalid = true;
    }

    int getHeaderFlag(int i) {
        if (proto.flag_size() > 0) {
            return proto.flag(i);
        } else {
            return combineHeader(SLOW::NORMAL);
        }
    }

    void setHeaderFlag(int i, int header) {
        proto.set_flag(i, header);
    }

    static int combineHeader(SLOW slow) {
        int header = 0;
        header = StatusFormat::setSLOW(header, slow);
        return header;
    }

    static SLOW getSLOWFromHeader(int header) {
        return StatusFormat::getSLOW(header);
    }

    long getDiskTime(int i) {
        if (proto.disktimenanos_size() > 0) {
            return proto.disktimenanos(i);
        } else {
            return 0L;
        }
    }

    void setDiskTime(int i, int diskTime) {
        proto.set_disktimenanos(i, diskTime);
    }

    long getDownstreamTime(int i) {
        if (proto.downstreamtimenanos_size() > 0) {
            return proto.downstreamtimenanos(i);
        } else {
            return 0L;
        }
    }

    void setDownstreamTime(int i, int downstreamTime) {
        proto.set_downstreamtimenanos(i, downstreamTime);
    }

private:
    PipelineAckProto proto;
    bool invalid;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PIPELINEACK_H_ */
