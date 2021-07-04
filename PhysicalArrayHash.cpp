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

/*
namespace array_hash
{


template <Settings::SchemaType SCHEMA_TYPE>
class MergeWriter : public boost::noncopyable
{
private:
    shared_ptr<Array> _output;
    size_t const _groupSize;
    size_t const _numAggs;
    size_t const _chunkSize;
    size_t const _numInstances;
    InstanceID const _myInstanceId;
    vector<uint32_t> _hashBreaks;
    size_t _currentBreak;
    shared_ptr<Query> _query;
    Settings& _settings;
    Coordinates _outputPosition;
    Coordinate& _outputValueNo;
    shared_ptr<ArrayIterator> _hashArrayIterator;
    shared_ptr<ChunkIterator> _hashChunkIterator;
    vector<shared_ptr<ArrayIterator> > _groupArrayIterators;
    vector<shared_ptr<ChunkIterator> > _groupChunkIterators;
    vector<shared_ptr<ArrayIterator> >_itemArrayIterators;
    vector<shared_ptr<ChunkIterator> >_itemChunkIterators;
    Value            _curHash;
    vector<Value>    _curGroup;
    vector<Value>    _curStates;

public:
    MergeWriter(Settings& settings, shared_ptr<Query> const& query, string const name = ""):
        _output(std::make_shared<MemArray>(settings.makeSchema(query,SCHEMA_TYPE, name), query)),
        _groupSize(settings.getGroupSize()),
        _numAggs(settings.getNumAggs()),
        _chunkSize(_output->getArrayDesc().getDimensions()[_output->getArrayDesc().getDimensions().size()-1].getChunkInterval()),
        _numInstances(query->getInstancesCount()),
        _myInstanceId(query->getInstanceID()),
        _hashBreaks(_numInstances-1,0),
        _query(query),
        _settings(settings),
        _outputPosition( SCHEMA_TYPE == Settings::SPILL ? 1 :
                         SCHEMA_TYPE== Settings::MERGE ?  3 :
                                                          2 , 0),
        _outputValueNo(  SCHEMA_TYPE == Settings::SPILL ? _outputPosition[0] :
                         SCHEMA_TYPE == Settings::MERGE ? _outputPosition[2] :
                                                          _outputPosition[1]),
        _hashArrayIterator(NULL),
        _hashChunkIterator(NULL),
        _groupArrayIterators(_groupSize, NULL),
        _groupChunkIterators(_groupSize, NULL),
        _itemArrayIterators(_numAggs, NULL),
        _itemChunkIterators(_numAggs, NULL),
        _curGroup(_groupSize),
        _curStates(_numAggs)
    {
        _curHash.setNull(0);
        for(size_t i=0; i<_groupSize; ++i)
        {
            _curGroup[i].setNull(0);
        }
        for(size_t i=0; i<_numAggs; ++i)
        {
            _curStates[i].setNull(0);
        }
        uint32_t break_interval = safe_static_cast<uint32_t>(
            _settings.getNumHashBuckets() / _numInstances); //XXX:CAN'T DO EASY ROUNDOFF
        for(uint32_t i=0; i<_numInstances-1; ++i)
        {
            _hashBreaks[i] = break_interval * (i+1);
        }
        _currentBreak = 0;
        if(SCHEMA_TYPE == Settings::MERGE)
        {
            _outputPosition[0] = 0;
            _outputPosition[1] = _myInstanceId;
            _outputPosition[2] = 0;
        }
        else if(SCHEMA_TYPE == Settings::FINAL)
        {
            _outputPosition[0] = _myInstanceId;
            _outputPosition[1] = 0;
        }
        AttributeID i = 0;
        if(SCHEMA_TYPE != Settings::FINAL)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _hashArrayIterator = _output->getIterator(*aidIter);
            ++i;
        }
        for(size_t j =0; j<_groupSize; ++j)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _groupArrayIterators[j] = _output->getIterator(*aidIter);
            ++i;
        }
        for(size_t j=0; j<_numAggs; ++j)
        {
            auto aidIter = _output->getArrayDesc().getAttributes().find(i);
            _itemArrayIterators[j] = _output->getIterator(*aidIter);
            ++i;
        }
    }

private:
    void copyGroup( vector<Value const*> const& group)
    {
        for(size_t i =0; i<_groupSize; ++i)
        {
            _curGroup[i] = *(group[i]);
        }
    }

public:
    void writeValue (uint32_t const hash, vector<Value const*> const& group, vector<Value const*> const& inputs)
    {
        Value buf;
        buf.setUint32(hash);
        writeValue(buf, group, inputs);
    }

    void writeValue (Value const& hash, vector<Value const*> const& group, vector<Value const*> const& inputs)
    {
        if(SCHEMA_TYPE == Settings::SPILL )
        {
            if(_curHash.getMissingReason() != 0)
            {
                writeCurrent();
            }
            _curHash = hash;
            copyGroup(group);
            for(size_t i =0; i<_numAggs; ++i)
            {
                _curStates[i] = *(inputs[i]);
            }
        }
        else
        {
            if(_curHash.getMissingReason() == 0 || _curHash.getUint32() != hash.getUint32() || !_settings.groupEqual(&(_curGroup[0]), group))
            {
                if(_curHash.getMissingReason() != 0)
                {
                    writeCurrent();
                }
                _curHash = hash;
                copyGroup(group);
                _settings.aggInitState(&(_curStates[0]));
            }
            _settings.aggAccumulate(&(_curStates[0]), inputs);
        }
    }

    void writeState (uint32_t const hash, vector<Value const*> const& group, vector<Value const*> const& states)
    {
        Value buf;
        buf.setUint32(hash);
        writeState(buf, group, states);
    }

    void writeState (Value const& hash, vector<Value const*> const& group, vector<Value const*> const& states)
    {
        if(SCHEMA_TYPE == Settings::SPILL)
        {
            throw SYSTEM_EXCEPTION(SCIDB_SE_INTERNAL, SCIDB_LE_ILLEGAL_OPERATION) << "don't call writeState on a SPILL writer";
        }
        else
        {
            if(_curHash.getMissingReason() == 0 || _curHash.getUint32() != hash.getUint32() || !_settings.groupEqual(&(_curGroup[0]), group))
            {
                if(_curHash.getMissingReason() != 0)
                {
                    writeCurrent();
                }
                _curHash = hash;
                copyGroup(group);
                _settings.aggInitState(&(_curStates[0]));
            }
            _settings.aggMerge(&(_curStates[0]), states);
        }
    }

private:
    void writeCurrent()
    {
        //gonna do a write, then!
        while( SCHEMA_TYPE == Settings::MERGE && _currentBreak < _numInstances - 1 && _curHash.getUint32() > _hashBreaks[_currentBreak] )
        {
            ++_currentBreak;
        }
        bool newChunk = false;
        if ( SCHEMA_TYPE == Settings::MERGE && static_cast<Coordinate>(_currentBreak) != _outputPosition[0])
        {
            _outputPosition[0] = _currentBreak;
            _outputPosition[2] = 0;
            newChunk = true;
        }
        else if( _outputValueNo % _chunkSize == 0)
        {
            newChunk = true;
        }
        if( newChunk )
        {
            size_t i = 0;
            if(SCHEMA_TYPE != Settings::FINAL)
            {
                if(_hashChunkIterator.get())
                {
                    _hashChunkIterator->flush();
                }
                _hashChunkIterator = _hashArrayIterator -> newChunk(_outputPosition).getIterator(_query,
                                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
            for(size_t j =0; j<_groupSize; ++j)
            {
                if(_groupChunkIterators[j].get())
                {
                    _groupChunkIterators[j]->flush();
                }
                _groupChunkIterators[j] = _groupArrayIterators[j]->newChunk(_outputPosition).getIterator(_query,
                                                i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
            for(size_t j =0; j<_numAggs; ++j)
            {
                if(_itemChunkIterators[j].get())
                {
                    _itemChunkIterators[j]->flush();
                }
                _itemChunkIterators[j] = _itemArrayIterators[j] -> newChunk(_outputPosition).getIterator(_query,
                                               i == 0 ? ChunkIterator::SEQUENTIAL_WRITE : ChunkIterator::SEQUENTIAL_WRITE | ChunkIterator::NO_EMPTY_CHECK);
                ++i;
            }
        }
        if(SCHEMA_TYPE != Settings::FINAL)
        {
            _hashChunkIterator->setPosition(_outputPosition);
            _hashChunkIterator->writeItem(_curHash);
        }
        for(size_t j =0; j<_groupSize; ++j)
        {
            _groupChunkIterators[j]->setPosition(_outputPosition);
            _groupChunkIterators[j]->writeItem(_curGroup[j]);
        }
        if(SCHEMA_TYPE == Settings::FINAL)
        {
            vector<Value> result(_numAggs);
            _settings.aggFinal(&(result[0]), &(_curStates[0]));
            for (size_t j=0; j<_numAggs; ++j)
            {
                _itemChunkIterators[j]->setPosition(_outputPosition);
                _itemChunkIterators[j]->writeItem(result[j]);
            }
        }
        else
        {
            for (size_t j=0; j<_numAggs; ++j)
            {
                _itemChunkIterators[j]->setPosition(_outputPosition);
                _itemChunkIterators[j]->writeItem(_curStates[j]);
            }
        }
        ++_outputValueNo;
    }

public:
    shared_ptr<Array> finalize()
    {
        if(_curHash.getMissingReason() != 0)
        {
            writeCurrent();
        }
        if(SCHEMA_TYPE != Settings::FINAL && _hashChunkIterator.get())
        {
            _hashChunkIterator->flush();
        }
        _hashChunkIterator.reset();
        _hashArrayIterator.reset();
        for(size_t j =0; j<_groupSize; ++j)
        {
            if(_groupChunkIterators[j].get())
            {
                _groupChunkIterators[j]->flush();
            }
            _groupChunkIterators[j].reset();
            _groupArrayIterators[j].reset();
        }
        for(size_t j =0; j<_numAggs; ++j)
        {
            if(_itemChunkIterators[j].get())
            {
                _itemChunkIterators[j]->flush();
            }
            _itemChunkIterators[j].reset();
            _itemArrayIterators[j].reset();
        }
        shared_ptr<Array> result = _output;
        _output.reset();
        return result;
    }
};



} //namespace array_hash
*/

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
        LOG4CXX_INFO(logger, "AH finished local sum "<<result.sum<<" prod "<<result.prod<<" exor "<<result.exor<<" count "<<result.count);
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
            LOG4CXX_INFO(logger, "AH finished merge sum "<<globalResult.sum<<" prod "<<globalResult.prod<<" exor "<<globalResult.exor<<" count "<<globalResult.count);
        }
        else
        {
            LOG4CXX_INFO(logger, "AH finished merge, not coordiantor");
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
                LOG4CXX_INFO(logger, "AH finished merge hash "<<resultHash);
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
