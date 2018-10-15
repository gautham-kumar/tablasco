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
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.block.factory.Comparators;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

public class AdaptivePartialMatcher implements PartialMatcher
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AdaptivePartialMatcher.class);

    private final ComparableTable rhsData;
    private final ComparableTable lhsData;
    private final ColumnComparators columnComparators;
    private final long bestMatchThreshold;

    public AdaptivePartialMatcher(ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators, int bestMatchThreshold)
    {
        this.rhsData = rhsData;
        this.lhsData = lhsData;
        this.columnComparators = columnComparators;
        this.bestMatchThreshold = (long) bestMatchThreshold;
    }

    @Override
    public void match(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> matchedColumns)
    {
        this.groupAndMatch(allMissingRows, allSurplusRows, matchedColumns, null, 0);
    }

    private void groupAndMatch(MutableList<UnmatchedIndexMap> missingRows, MutableList<UnmatchedIndexMap> surplusRows, MutableList<IndexMap> matchedColumns, MutableList<IndexMap> columnsOrderedBySelectivity, int columnIndex)
    {
        if ((long) missingRows.size() * (long) surplusRows.size() <= this.bestMatchThreshold)
        {
            LOGGER.debug("Matching {} missing and {} surplus rows using best-match algorithm", missingRows.size(), surplusRows.size());
            new BestMatchPartialMatcher(this.rhsData, this.lhsData, this.columnComparators).match(missingRows, surplusRows, matchedColumns);
            return;
        }
        MutableList<IndexMap> initializedColumnsOrderedBySelectivity = columnsOrderedBySelectivity;
        if (columnIndex == 0)
        {
            initializedColumnsOrderedBySelectivity = getColumnsOrderedBySelectivity(missingRows, surplusRows, matchedColumns);
        }
        if (columnIndex >= initializedColumnsOrderedBySelectivity.size())
        {
            LOGGER.info("Matching remaining {} missing and {} surplus rows using best-match algorithm", missingRows.size(), surplusRows.size());
            new BestMatchPartialMatcher(this.rhsData, this.lhsData, this.columnComparators).match(missingRows, surplusRows, matchedColumns);
            return;
        }
        IndexMap column = initializedColumnsOrderedBySelectivity.get(columnIndex);
        LOGGER.info("Grouping by '{}' column", this.rhsData.getColumnName(column.getRhsIndex()));
        CellComparator lhsComparator = this.columnComparators.getComparator(lhsData.getColumnName(column.getLhsIndex()));
        MutableListMultimap<String, UnmatchedIndexMap> missingRowsByColumn = missingRows.groupBy(Functions.chain(lhsValueFunction(column), lhsComparator.getFormatter()));
        CellComparator rhsComparator = this.columnComparators.getComparator(rhsData.getColumnName(column.getRhsIndex()));
        MutableListMultimap<String, UnmatchedIndexMap> surplusRowsByColumn = surplusRows.groupBy(Functions.chain(rhsValueFunction(column), rhsComparator.getFormatter()));
        for (String key : missingRowsByColumn.keysView())
        {
            LOGGER.debug("Matching '{}'", key);
            MutableList<UnmatchedIndexMap> missingByKey = missingRowsByColumn.get(key);
            MutableList<UnmatchedIndexMap> surplusByKey = surplusRowsByColumn.get(key);
            if (surplusByKey != null)
            {
                groupAndMatch(missingByKey, surplusByKey, matchedColumns, initializedColumnsOrderedBySelectivity, columnIndex + 1);
            }
        }
    }

    private Function<UnmatchedIndexMap, Object> rhsValueFunction(final IndexMap column)
    {
        return object -> AdaptivePartialMatcher.this.rhsData.getValueAt(object.getRhsIndex(), column.getRhsIndex());
    }

    private Function<UnmatchedIndexMap, Object> lhsValueFunction(final IndexMap column)
    {
        return object -> AdaptivePartialMatcher.this.lhsData.getValueAt(object.getLhsIndex(), column.getLhsIndex());
    }

    private MutableList<IndexMap> getColumnsOrderedBySelectivity(MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows, MutableList<IndexMap> columnIndices)
    {
        LOGGER.info("Calculating column selectivity");
        MutableList<Pair<IndexMap, Integer>> columnSelectivities = Lists.mutable.of();
        for (IndexMap column : columnIndices)
        {
            CellComparator lhsComparator = this.columnComparators.getComparator(lhsData.getColumnName(column.getLhsIndex()));
            Set<String> lhsValues = getColumnValues(allMissingRows, Functions.chain(lhsValueFunction(column), lhsComparator.getFormatter()));
            CellComparator rhsComparator = this.columnComparators.getComparator(rhsData.getColumnName(column.getRhsIndex()));
            Set<String> rhsValues = getColumnValues(allSurplusRows, Functions.chain(rhsValueFunction(column), rhsComparator.getFormatter()));
            rhsValues.retainAll(lhsValues);
            int selectivity = rhsValues.size();
            if (selectivity > 0)
            {
                columnSelectivities.add(Tuples.pair(column, Integer.valueOf(selectivity)));
            }
        }
        return columnSelectivities
                .sortThis(Comparators.reverse(Comparators.byFunction(Functions.<Integer>secondOfPair())))
                .collect(Functions.<IndexMap>firstOfPair());
    }

    private static Set<String> getColumnValues(MutableList<UnmatchedIndexMap> rows, Function<UnmatchedIndexMap, String> valueFunction)
    {
        if (!CellComparator.isFloatingPoint(valueFunction.valueOf(rows.getFirst())))
        {
            Set<String> values = UnifiedSet.newSet();
            rows.collect(valueFunction, values);
            return values;
        }
        return Collections.emptySet();
    }
}
