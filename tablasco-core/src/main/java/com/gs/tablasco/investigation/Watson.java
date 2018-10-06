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

package com.gs.tablasco.investigation;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.compare.*;
import com.gs.tablasco.compare.indexmap.IndexMapTableComparator;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.Iterate;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Watson "assists" Sherlock in reconciling two environments for one level of group bys,
 * returning the drilldown keys of breaks only.
 */
class Watson
{
    private final SingleTableComparator tableComparator;
    private final Procedure2<String, Map<String, ResultTable>> appendToHtml;

    public Watson(Procedure2<String, Map<String, ResultTable>> appendToHtml)
    {
        this.appendToHtml = appendToHtml;
        ColumnComparators columnComparators = new ColumnComparators.Builder().withTolerance(1.0).build();
        this.tableComparator = new IndexMapTableComparator(columnComparators, false, IndexMapTableComparator.DEFAULT_BEST_MATCH_THRESHOLD);
    }

    public List<Object> assist(String levelName, InvestigationLevel nextLevel, int drilldownLimit)
    {
        Twin<ComparableTable> queryResults = execute(nextLevel);
        ComparableTable rhsResults = new KeyedComparableTableAdapter(queryResults.getOne(), queryResults.getOne().getColumnCount() - 1);
        ComparableTable lhsResults = new KeyedComparableTableAdapter(queryResults.getTwo(), queryResults.getTwo().getColumnCount() - 1);

        List<String> rhsColumns = getColumns(rhsResults);
        List<String> lhsColumns = getColumns(lhsResults);
        if (!Iterate.getLast(rhsColumns).equals(Iterate.getLast(lhsColumns)))
        {
            throw new IllegalArgumentException(String.format("Key columns must match at each investigation level [rhs: %s, lhs: %s]", Iterate.getLast(rhsColumns), Iterate.getLast(lhsColumns)));
        }
        Set<String> commonColumns = UnifiedSet.newSet(rhsColumns);
        commonColumns.retainAll(lhsColumns);
        if (Math.min(rhsColumns.size(), lhsColumns.size()) > 1 && commonColumns.size() < 2)
        {
            throw new IllegalArgumentException(String.format("There must be at least 2 matching columns at each investigation level [rhs: %s, lhs: %s]", Iterate.getLast(rhsColumns), Iterate.getLast(lhsColumns)));
        }

        String levelDescription = nextLevel.getLevelDescription();
        ResultTable results = this.tableComparator.compare(lhsResults, rhsResults);
        this.appendToHtml.value(levelName, Maps.fixedSize.of(levelDescription, results));
        return getRowKeys(results, drilldownLimit);
    }

    private List<Object> getRowKeys(ResultTable results, int drilldownLimit)
    {
        List<Object> rowKeys = FastList.newList();
        List<List<ResultCell>> table = results.getComparedRows();
        int rowIndex = 1;
        while (rowIndex < table.size() && rowKeys.size() < drilldownLimit)
        {
            List<ResultCell> values = table.get(rowIndex);
            int passedCount = Iterate.count(values, ResultCell.IS_PASSED_CELL);
            int failedCount = Iterate.count(values, ResultCell.IS_FAILED_CELL);
            if (passedCount == 0 || failedCount > 0)
            {
                ResultCell cell = Iterate.getLast(values);
                rowKeys.add(cell.getRhs() == null ? cell.getLhs() : cell.getRhs());
            }
            rowIndex++;
        }
        return rowKeys;
    }

    private List<String> getColumns(ComparableTable table)
    {
        List<String> cols = FastList.newList(table.getColumnCount());
        for (int i = 0; i < table.getColumnCount(); i++)
        {
            cols.add(table.getColumnName(i));
        }
        return cols;
    }

    private static Twin<ComparableTable> execute(InvestigationLevel investigationLevel)
    {
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        try
        {
            Future<ComparableTable> rhsFuture = executorService.submit(investigationLevel.getRhsResults());
            Future<ComparableTable> lhsFuture = executorService.submit(investigationLevel.getLhsResults());
            return Tuples.twin(rhsFuture.get(), lhsFuture.get());
        }
        catch (Exception e)
        {
            throw new RuntimeException("Error running queries", e);
        }
        finally
        {
            executorService.shutdown();
        }
    }
}
