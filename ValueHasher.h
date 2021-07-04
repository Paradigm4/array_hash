/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2008-2021 Paradigm4, Inc.
* All Rights Reserved.
*
* array_hash is a plugin for SciDB. See http://www.paradigm4.com/
*
* array_hash is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* array_hash is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with array_hash.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

#ifndef VALUE_HASHER
#define VALUE_HASHER

#include <query/PhysicalOperator.h>

using std::vector;

namespace scidb
{
namespace array_hash
{

class ValueHasher
{
private:
    //-----------------------------------------------------------------------------
    // MurmurHash3 was written by Austin Appleby, and is placed in the public
    // domain. The author hereby disclaims copyright to this source code.
    #define ROT32(x, y) ((x << y) | (x >> (32 - y))) // avoid effort
    static uint32_t murmur3_32(const char *key, uint32_t len, uint32_t const seed = 0x5C1DB123)
    {
        static const uint32_t c1 = 0xcc9e2d51;
        static const uint32_t c2 = 0x1b873593;
        static const uint32_t r1 = 15;
        static const uint32_t r2 = 13;
        static const uint32_t m = 5;
        static const uint32_t n = 0xe6546b64;
        uint32_t hash = seed;
        const int nblocks = len / 4;
        const uint32_t *blocks = (const uint32_t *) key;
        int i;
        uint32_t k;
        for (i = 0; i < nblocks; i++)
        {
            k = blocks[i];
            k *= c1;
            k = ROT32(k, r1);
            k *= c2;
            hash ^= k;
            hash = ROT32(hash, r2) * m + n;
        }
        const uint8_t *tail = (const uint8_t *) (key + nblocks * 4);
        uint32_t k1 = 0;
        switch (len & 3)
        {
        case 3:
            k1 ^= tail[2] << 16;
        case 2:
            k1 ^= tail[1] << 8;
        case 1:
            k1 ^= tail[0];
            k1 *= c1;
            k1 = ROT32(k1, r1);
            k1 *= c2;
            hash ^= k1;
        }
        hash ^= len;
        hash ^= (hash >> 16);
        hash *= 0x85ebca6b;
        hash ^= (hash >> 13);
        hash *= 0xc2b2ae35;
        hash ^= (hash >> 16);
        return hash;
    }
    //End of MurmurHash3 Implementation
    //-----------------------------------------------------------------------------

    mutable vector<char>                     _hashBuf;

public:
    ValueHasher():
        _hashBuf(64)
    {}

    uint32_t hashTuple(std::vector<Value const*> const& tuple) const
    {
        uint32_t totalSize = 0;
        for(size_t i =0; i<tuple.size(); ++i)
        {
            if(tuple[i] ->isNull())
            {
                totalSize += sizeof(Value::reason);
            }
            else
            {
                totalSize += sizeof(Value::reason);
                totalSize += tuple[i]->size();
            }
        }
        if(_hashBuf.size() < totalSize)
        {
            _hashBuf.resize(totalSize);
        }
        char* ch = &_hashBuf[0];
        for(size_t i =0; i<tuple.size(); ++i)
        {
            Value::reason nullBit = -1;
            if(tuple[i] ->isNull())
            {
                nullBit = tuple[i]->getMissingReason();
            }
            memcpy(ch, &nullBit, sizeof(Value::reason));
            ch += sizeof(Value::reason);
            if( !tuple[i] ->isNull())
            {
                memcpy(ch, tuple[i]->data(), tuple[i]->size());
                ch += tuple[i]->size();
            }
        }
        return murmur3_32(&_hashBuf[0], totalSize);
    }
};

} } //namespace scidb::array_hash

#endif
