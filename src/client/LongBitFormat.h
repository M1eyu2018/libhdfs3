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

#ifndef LIBHDFS3_LONGBITFORMAT_H
#define LIBHDFS3_LONGBITFORMAT_H

#include <string>

#include "Exception.h"
#include "ExceptionInternal.h"

namespace Hdfs {
namespace Internal {

class LongBitFormat {

public:
    LongBitFormat(std::string name, int offset, int length, long min) {
        this->name = name;
        this->offset = offset;
        this->length = length;
        this->min = min;
        this->max = ((1L) << this->length) - 1;
        this->mask = this->max << this->offset;
    }

    /** Retrieve the value from the record. */
    long retrieve(long record) {
        return (record >> this->offset) & (this->max);
    }

    /** Combine the value to the record. */
    long combine(long value, long record) {
        if (value < this->min) {
            THROW(HadoopIllegalArgumentException, "Illagal value: %s = %ld < MIN = %ld", this->name.c_str(), value, this->min);
        }
        if (value > this->max) {
            THROW(HadoopIllegalArgumentException, "Illagal value: %s = %ld > MAX = %ld", this->name.c_str(), value, this->max);
        }
        return (record & ~(this->mask)) | (value << this->offset);
    }

private:
    std::string name;
    /** Bit offset */
    int offset;
    /** Bit length */
    int length;
    /** Minimum value */
    long min;
    /** Maximum value */
    long max;
    /** Bit mask */
    long mask;
};

}
}

#endif //LIBHDFS3_LONGBITFORMAT_H
