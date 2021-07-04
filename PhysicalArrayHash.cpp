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

#include <query/PhysicalOperator.h>
#include <array/Dimensions.h>
#include <array/Metadata.h>
#include <system/Cluster.h>
#include <query/Query.h>
#include <system/Exceptions.h>
#include <system/Utils.h>
#include <log4cxx/logger.h>
#include <network/NetworkMessage.h>
#include <array/MemArray.h>
#include <network/Network.h>

#include <array/ArrayIterator.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <cstddef>

#include "ValueHasher.h"

using namespace std;

namespace scidb
{

using namespace array_hash;
static log4cxx::LoggerPtr logger(log4cxx::Logger::getLogger("scidb.operators.array_hash"));

class PhyiscalArrayHash : public PhysicalOperator
{

public:
    PhyiscalArrayHash(string const& logicalName,
                      string const& physicalName,
                      Parameters const& parameters,
                      ArrayDesc const& schema):
        PhysicalOperator(logicalName, physicalName, parameters, schema)
    {}

    /// @see OperatorDist
    DistType inferSynthesizedDistType(std::vector<DistType> const& /*inDist*/, size_t /*depth*/) const override
    {
      // Distribution is undefined.
      SCIDB_ASSERT(_schema.getDistribution()->getDistType()==dtUndefined);
      return _schema.getDistribution()->getDistType();
    }

    /// @see PhysicalOperator
    virtual RedistributeContext getOutputDistribution(vector<RedistributeContext> const& inputDistributions,
                                                      vector<ArrayDesc> const& inputSchemas) const override
    {
      assertConsistency(inputSchemas[0], inputDistributions[0]);

      // Distribution is undefined.
      SCIDB_ASSERT(_schema.getDistribution()->getDistType()==dtUndefined);
      _schema.setResidency(inputDistributions[0].getArrayResidency());

      return RedistributeContext(_schema.getDistribution(), _schema.getResidency());
    }

    struct IntermediateHashes
    {
        uint32_t sum;
        uint32_t prod;
        uint32_t exor;
        uint64_t count;
    };

    static uint32_t const LARGE_PRIME = 4294967291;

    void hashLocalData (shared_ptr<Array>& input, IntermediateHashes& result)
    {
        result.count = 0;
        ArrayDesc const& schema = input->getArrayDesc();
        auto attributes = schema.getAttributes(/*excludeEbm:*/true);
        size_t const numAttributes = attributes.size();
        auto dimensions = schema.getDimensions();
        size_t const numDimensions = isDataframe(dimensions) ? 0 : dimensions.size();
        vector<Value const*> tuple(numDimensions + numAttributes, NULL);
        vector<Value> dimValues (numDimensions);
        for(size_t j = 0; j<numDimensions; ++j)
        {
            tuple[j] = &(dimValues[j]);
        }
        vector<shared_ptr<ConstArrayIterator> > iaiters(numAttributes);
        vector<shared_ptr<ConstChunkIterator> > iciters(numAttributes);
        scidb::array_hash::ValueHasher hasher;
        size_t i = 0;
        for (auto attr : attributes) //TODO: will this iterate over attributes in schema-order?
        {
            iaiters[i] = input->getConstIterator( attr );
            i ++;
        }
        while( !(iaiters[0]->end()) )
        {
            for( i =0; i<numAttributes; ++i)
            {
                iciters[i] = iaiters[i]->getChunk().getConstIterator();
            }
            while (!iciters[0]->end())
            {
                Coordinates const& pos = iciters[0]->getPosition();
                size_t j = 0;
                for( ; j<numDimensions; ++j)
                {
                    dimValues[j].setInt64( pos[j] );
                }
                for( i =0; i<numAttributes; ++i)
                {
                    tuple[i+j] = &(iciters[i]->getItem());
                }
                uint32_t hash = hasher.hashTuple(tuple);
                if(hash == 0 || hash == LARGE_PRIME)
                {
                    //TODO: mmebe? just tryna protect against a 0 polluting the product
                    //this could use a review by someone who like knows math
                    hash--;
                }
                if(result.count == 0)
                {
                    result.sum = hash;
                    result.prod = hash;
                    result.exor = hash;
                }
                else
                {
                    result.sum  = result.sum  + hash;
                    result.prod = ((uint64_t) result.prod * (uint64_t) hash) % LARGE_PRIME;
                    result.exor = result.exor ^ hash;
                }
                result.count++;
                for( i =0; i<numAttributes; ++i)
                {
                    ++( *(iciters[i]));
                }
            }
            for( i =0; i<numAttributes; ++i)
            {
                ++( *(iaiters[i]));
            }
        }
        LOG4CXX_DEBUG(logger, "AH finished local sum "<<result.sum<<" prod "<<result.prod<<" exor "<<result.exor<<" count "<<result.count);
    }

    void mergeResults (IntermediateHashes const& localResult,
                       shared_ptr<Query>& query,
                       IntermediateHashes& globalResult)
    {
        if ( !(query->isCoordinator()))
        {
            InstanceID coordinator = query->getCoordinatorID();
            shared_ptr<SharedBuffer> buf(new MemoryBuffer(NULL, sizeof(IntermediateHashes)));
            *((IntermediateHashes*) buf->getWriteData()) = localResult;
            BufSend(coordinator, buf, query);
        }
        else
        {
            globalResult = localResult;
            size_t const nInstances = query->getInstancesCount();
            InstanceID const myId = query->getInstanceID();
            for (InstanceID i=0; i<nInstances; ++i)
            {
                if(i != myId)
                {
                    shared_ptr<SharedBuffer> inBuf = BufReceive(i,query);
                    IntermediateHashes otherInstanceResult = *((IntermediateHashes*) inBuf->getWriteData());
                    if (otherInstanceResult.count > 0)
                    {
                        if (globalResult.count == 0)
                        {
                            globalResult = otherInstanceResult;
                        }
                        else
                        {
                            globalResult.sum   = globalResult.sum   + otherInstanceResult.sum;
                            globalResult.prod  = ((uint64_t) globalResult.prod  * (uint64_t) otherInstanceResult.prod) % LARGE_PRIME;
                            globalResult.exor  = globalResult.exor  ^ otherInstanceResult.exor;
                            globalResult.count = globalResult.count + otherInstanceResult.count;
                        }
                    }
                }
            }
        }
        if (query->isCoordinator())
        {
            LOG4CXX_DEBUG(logger, "AH finished merge sum "<<globalResult.sum<<" prod "<<globalResult.prod<<" exor "<<globalResult.exor<<" count "<<globalResult.count);
        }
        else
        {
            LOG4CXX_DEBUG(logger, "AH finished merge, not coordiantor");
        }
    }

    shared_ptr< Array> execute(vector< shared_ptr< Array> >& inputArrays, shared_ptr<Query> query)
    {
        shared_ptr<Array> input = inputArrays[0];
        IntermediateHashes hashes;
        hashLocalData(input, hashes);
        IntermediateHashes globalHashes;
        mergeResults(hashes, query, globalHashes);
        shared_ptr<Array> result (new MemArray(_schema, query));
        if (query->isCoordinator())
        {
            Coordinates position(2,0);
            auto const& attrs = _schema.getAttributes(true);
            auto attrIter = attrs.begin();
            shared_ptr<ArrayIterator> dataHashAiter = result->getIterator(*attrIter);
            shared_ptr<ChunkIterator> dataHashCiter = dataHashAiter->newChunk(position).getIterator(query, ChunkIterator::SEQUENTIAL_WRITE);
            dataHashCiter->setPosition(position);
            Value buf;
            buf.setNull();
            if(globalHashes.count>0)
            {
                string resultHash;
                ostringstream stream;
                stream << hex
                       << setw(sizeof(globalHashes.sum)*2)
                       << setfill ('0')
                       << globalHashes.sum
                       << setw(sizeof(globalHashes.prod)*2)
                       << setfill ('0')
                       << globalHashes.prod
                       << setw(sizeof(globalHashes.exor)*2)
                       << setfill ('0')
                       << globalHashes.exor;
                resultHash =  stream.str();
                buf.setString(resultHash);
            }
            dataHashCiter->writeItem(buf);
            dataHashCiter->flush();
            attrIter++;
            shared_ptr<ArrayIterator> countAiter = result->getIterator(*attrIter);
            shared_ptr<ChunkIterator> countCiter = countAiter->newChunk(position).getIterator(query,
                                                                        ChunkIterator::SEQUENTIAL_WRITE |
                                                                        ChunkIterator::NO_EMPTY_CHECK);
            buf.setUint64(globalHashes.count);
            countCiter->writeItem(buf);
            countCiter->flush();
        }
        return result;
    }
};

REGISTER_PHYSICAL_OPERATOR_FACTORY(PhyiscalArrayHash, "array_hash", "physical_array_hash");
} //namespace scidb
