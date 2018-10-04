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
import com.gs.tablasco.compare.ColumnComparators;
import com.gs.tablasco.compare.KeyedComparableTable;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.utility.Iterate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class KeyColumnPartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(KeyColumnPartialMatcher.class);

    private static final Function0<MutableList<UnmatchedIndexMap>> NEW_LIST = () -> FastList.newList(4);

    private final KeyedComparableTable rhsData;
    private final ComparableTable lhsData;
    private final ColumnComparators columnComparators;
    private final PartialMatcher keyGroupPartialMatcher;

    public KeyColumnPartialMatcher(KeyedComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators, PartialMatcher keyGroupPartialMatcher)
    {
        this.rhsData = rhsData;
        this.lhsData = lhsData;
        this.columnComparators = columnComparators;
        this.keyGroupPartialMatcher = keyGroupPartialMatcher;
    }

    @Override
    public void match(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> matchedColumns)
    {
        List<IndexMap> keyColumnIndices = this.getKeyColumnIndexMaps(matchedColumns);
        if (keyColumnIndices.isEmpty())
        {
            LOGGER.warn("No key columns found!");
            return;
        }
        MutableMap<RowView, MutableList<UnmatchedIndexMap>> missingByKey = UnifiedMap.newMap(allMissingRows.size());
        for (UnmatchedIndexMap lhs : allMissingRows)
        {
            LhsRowView lhsRowView = new LhsRowView(this.lhsData, keyColumnIndices, this.columnComparators, lhs.getLhsIndex());
            missingByKey.getIfAbsentPut(lhsRowView, NEW_LIST).add(lhs);
        }
        MutableMap<RowView, MutableList<UnmatchedIndexMap>> surplusByKey = UnifiedMap.newMap(allSurplusRows.size());
        for (UnmatchedIndexMap rhs : allSurplusRows)
        {
            RhsRowView rhsRowView = new RhsRowView(this.rhsData, keyColumnIndices, this.columnComparators, rhs.getRhsIndex());
            surplusByKey.getIfAbsentPut(rhsRowView, NEW_LIST).add(rhs);
        }
        for (RowView rowView : missingByKey.keysView())
        {
            MutableList<UnmatchedIndexMap> missing = missingByKey.get(rowView);
            MutableList<UnmatchedIndexMap> surplus = surplusByKey.get(rowView);
            if (Iterate.notEmpty(missing) && Iterate.notEmpty(surplus))
            {
                this.keyGroupPartialMatcher.match(missing, surplus, matchedColumns);
            }
        }
    }

    private List<IndexMap> getKeyColumnIndexMaps(List<IndexMap> columnIndices)
    {
        List<IndexMap> keyColumns = FastList.newList(columnIndices.size());
        for (IndexMap columnIndexMap : columnIndices)
        {
            if (columnIndexMap.isMatched() && this.rhsData.isKeyColumn(columnIndexMap.getRhsIndex()))
            {
                keyColumns.add(columnIndexMap);
            }
        }
        return keyColumns;
    }
}