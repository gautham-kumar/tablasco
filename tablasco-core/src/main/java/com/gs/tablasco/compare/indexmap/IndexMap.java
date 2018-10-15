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

package com.gs.tablasco.compare.indexmap;

public class IndexMap implements Comparable<IndexMap>
{
    private final int lhsIndex;
    private final int rhsIndex;
    private boolean isOutOfOrder;

    public IndexMap(int lhsIndex, int rhsIndex)
    {
        this.lhsIndex = lhsIndex;
        this.rhsIndex = rhsIndex;
        this.isOutOfOrder = false;
        if (lhsIndex < 0 && rhsIndex < 0)
        {
            throw new IllegalStateException("Only one index can be negative: " + this);
        }
    }

    public void setOutOfOrder()
    {
        this.isOutOfOrder = true;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof IndexMap)
        {
            IndexMap that = (IndexMap) obj;
            return this.lhsIndex == that.lhsIndex && this.rhsIndex == that.rhsIndex;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return this.isMissing() ? this.lhsIndex : this.rhsIndex;
    }

    @Override
    public int compareTo(IndexMap that)
    {
        if (this.equals(that))
        {
            return 0;
        }
        if (this.isMatched())
        {
            if (that.rhsIndex >= 0)
            {
                return compareUnequals(this.rhsIndex, that.rhsIndex, this.isSurplus());
            }
            return compareUnequals(this.lhsIndex, that.lhsIndex, this.isSurplus());
        }
        if (this.isSurplus())
        {
            if (that.rhsIndex >= 0)
            {
                return compareUnequals(this.rhsIndex, that.rhsIndex, this.isSurplus());
            }
            return compareUnequals(this.rhsIndex, that.lhsIndex, this.isSurplus());
        }
        if (that.lhsIndex >= 0)
        {
            return compareUnequals(this.lhsIndex, that.lhsIndex, this.isSurplus());
        }
        return compareUnequals(this.lhsIndex, that.rhsIndex, this.isSurplus());
    }

    public boolean isMissing()
    {
        return this.lhsIndex >= 0 && this.rhsIndex < 0;
    }

    public boolean isSurplus()
    {
        return this.rhsIndex >= 0 && this.lhsIndex < 0;
    }

    public boolean isMatched()
    {
        return this.rhsIndex >= 0 && this.lhsIndex >= 0;
    }

    public boolean isOutOfOrder()
    {
        return this.isOutOfOrder;
    }

    private static int compareUnequals(int thisIndex, int thatIndex, boolean thisIsSurplus)
    {
        if (thisIndex < thatIndex)
        {
            return -1;
        }
        if (thisIndex > thatIndex)
        {
            return 1;
        }
        if (thisIsSurplus)
        {
            return -1;
        }
        return 1;
    }

    @Override
    public String toString()
    {
        return "IndexMap{" +
                "lhsIndex=" + lhsIndex +
                ", rhsIndex=" + rhsIndex +
                ", isOutOfOrder=" + isOutOfOrder +
                '}';
    }

    public int getLhsIndex()
    {
        return lhsIndex;
    }

    public int getRhsIndex()
    {
        return rhsIndex;
    }
}
