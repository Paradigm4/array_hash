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

#define LEGACY_API

#include "query/LogicalOperator.h"
#include <array/ArrayDistributionInterface.h>
#include <query/Query.h>

namespace scidb
{

using namespace std;

class LogicalArrayHash : public LogicalOperator
{
public:
    LogicalArrayHash(const string& logicalName, const string& alias):
        LogicalOperator(logicalName, alias)
    {
        //TODO
        _usage = "write me a usage, bro!\n";
    }

    static PlistSpec const* makePlistSpec()
    {
        static PlistSpec argSpec {
            { "", // positionals
              RE(PP(PLACEHOLDER_INPUT))
            }
        };
        return &argSpec;
    }

    ArrayDesc inferSchema(vector< ArrayDesc> schemas, shared_ptr< Query> query)
    {
        vector<DimensionDesc> dimensions(2);
        dimensions[0] = DimensionDesc("$inst",  0, 1, 1, 0);
        dimensions[1] = DimensionDesc("$seq",   0, 1, 100000, 0);

        Attributes attributes;
        attributes.push_back(AttributeDesc("data_hash",
                                           TID_STRING,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));
        attributes.push_back(AttributeDesc("count",
                                           TID_UINT64,
                                           AttributeDesc::IS_NULLABLE,
                                           CompressorType::NONE));

        return ArrayDesc("array_hash",
                         attributes.addEmptyTagAttribute(),
                         dimensions,
                         createDistribution(dtUndefined),
                         query->getDefaultArrayResidency());
    }
};

REGISTER_LOGICAL_OPERATOR_FACTORY(LogicalArrayHash, "array_hash");

}
