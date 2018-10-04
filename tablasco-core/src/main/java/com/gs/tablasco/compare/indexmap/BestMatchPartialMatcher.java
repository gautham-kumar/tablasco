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

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.compare.CellComparator;
import com.gs.tablasco.compare.ColumnComparators;
import org.eclipse.collections.api.list.MutableList;

public class BestMatchPartialMatcher implements PartialMatcher
{
    private final ComparableTable rhsData;
    private final ComparableTable lhsData;
    private final ColumnComparators columnComparators;

    public BestMatchPartialMatcher(ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators)
    {
        this.rhsData = rhsData;
        this.lhsData = lhsData;
        this.columnComparators = columnComparators;
    }

    @Override
    public void match(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> matchedColumns)
    {
        for (UnmatchedIndexMap lhs : allMissingRows)
        {
            for (UnmatchedIndexMap rhs : allSurplusRows)
            {
                int matchScore = 0;
                for (int colIndex = 0; colIndex < matchedColumns.size(); colIndex++)
                {
                    IndexMap column = matchedColumns.get(colIndex);
                    Object lhsValue = this.lhsData.getValueAt(lhs.getLhsIndex(), column.getLhsIndex());
                    Object rhsValue = this.rhsData.getValueAt(rhs.getRhsIndex(), column.getRhsIndex());
                    CellComparator comparator = this.columnComparators.getComparator(lhsData.getColumnName(column.getLhsIndex()));
                    if (comparator.equals(rhsValue, lhsValue))
                    {
                        int inverseColumnNumber = matchedColumns.size() - colIndex;
                        matchScore += inverseColumnNumber * inverseColumnNumber;
                    }
                }
                if (matchScore > 0)
                {
                    lhs.addMatch(matchScore, rhs);
                }
            }
        }
        UnmatchedIndexMap.linkBestMatches(allMissingRows);
    }
}
