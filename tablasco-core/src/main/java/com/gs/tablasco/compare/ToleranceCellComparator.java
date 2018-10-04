/*
 * Copyright 2017 Goldman Sachs.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.gs.tablasco.compare;

public class ToleranceCellComparator extends CellComparator
{
    public ToleranceCellComparator(CellFormatter formatter)
    {
        super(formatter);
    }

    @Override
    public boolean compare(Object rhs, Object lhs)
    {
        if (isFloatingPoint(lhs) && isFloatingPoint(rhs))
        {
            double rhsVal = ((Number) rhs).doubleValue();
            double lhsVal = ((Number) lhs).doubleValue();
            return Double.compare(lhsVal, rhsVal) == 0 || Math.abs(lhsVal - rhsVal) <= getFormatter().getTolerance();
        }
        return false;
    }

    public static double getDifference(Object rhs, Object lhs)
    {
        return ((Number) lhs).doubleValue() - ((Number) rhs).doubleValue();
    }
}
