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
import com.gs.tablasco.compare.*;
import org.eclipse.collections.api.block.function.Function2;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

public class IndexMapTableComparator implements SingleTableComparator
{
    private static final Logger LOGGER = LoggerFactory.getLogger(IndexMapTableComparator.class);

    public static final int DEFAULT_BEST_MATCH_THRESHOLD = 1000000;
    public static final long DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS = TimeUnit.MINUTES.toMillis(5L);

    private final ColumnComparators columnComparators;
    private final boolean verifyRowOrder;
    private final int bestMatchThreshold;
    private final boolean ignoreSurplusRows;
    private final boolean ignoreMissingRows;
    private final boolean ignoreSurplusColumns;
    private final boolean ignoreMissingColumns;
    private final long partialMatchTimeoutMillis;

    public IndexMapTableComparator(ColumnComparators columnComparators, boolean verifyRowOrder, int bestMatchThreshold)
    {
        this(columnComparators, verifyRowOrder, bestMatchThreshold, false, false);
    }

    public IndexMapTableComparator(ColumnComparators columnComparators, boolean verifyRowOrder, int bestMatchThreshold, boolean ignoreSurplusRows, boolean ignoreMissingRows)
    {
        this(columnComparators, verifyRowOrder, bestMatchThreshold, ignoreSurplusRows, ignoreMissingRows, false, false, DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS);
    }

    public IndexMapTableComparator(ColumnComparators columnComparators, boolean verifyRowOrder, int bestMatchThreshold, boolean ignoreSurplusRows, boolean ignoreMissingRows, boolean ignoreSurplusColumns, boolean ignoreMissingColumns, long partialMatchTimeoutMillis)
    {
        this.columnComparators = columnComparators;
        this.verifyRowOrder = verifyRowOrder;
        this.bestMatchThreshold = bestMatchThreshold;
        this.ignoreSurplusRows = ignoreSurplusRows;
        this.ignoreMissingRows = ignoreMissingRows;
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        this.ignoreMissingColumns = ignoreMissingColumns;
        this.partialMatchTimeoutMillis = partialMatchTimeoutMillis;
    }

    @Override
    public ResultTable compare(ComparableTable rhsData, ComparableTable lhsData)
    {
        if (rhsData == null)
        {
            return new ResultTable(new boolean[lhsData.getColumnCount()], toListOfRows(lhsData,
                    (columnName, value) -> ResultCell.createMissingCell(columnComparators.getComparator(columnName).getFormatter(), value)));
        }
        if (lhsData == null)
        {
            return new ResultTable(new boolean[rhsData.getColumnCount()], toListOfRows(rhsData,
                    (columnName, value) -> ResultCell.createSurplusCell(columnComparators.getComparator(columnName).getFormatter(), value)));
        }

        LOGGER.info("Verifying {} col {} row rhs and {} col {} row lhs tables", rhsData.getColumnCount(), rhsData.getRowCount(), lhsData.getColumnCount(), lhsData.getRowCount());

        LOGGER.debug("Generating column indices");
        MutableList<IndexMap> columnIndices = getColumnIndices(rhsData, lhsData, columnComparators.getDefaultComparator());
        identifyOutOfOrderIndices(columnIndices, 0);

        boolean[] keyColumns = new boolean[columnIndices.size()];
        for (int i = 0; i < keyColumns.length; i++)
        {
            keyColumns[i] = rhsData instanceof KeyedComparableTable && ((KeyedComparableTable) rhsData).isKeyColumn(columnIndices.get(i).getRhsIndex());
        }

        List<List<ResultCell>> results = FastList.newList(rhsData.getRowCount() + 1);
        verifyHeaders(columnIndices, results, rhsData, lhsData, columnComparators.getDefaultComparator());

        LOGGER.debug("Starting Happy Path");
        collectMatchingRows(columnIndices, results, rhsData, lhsData, columnComparators);
        int happyPathSize = results.size() - 1; // minus headers
        if (happyPathSize == rhsData.getRowCount() && happyPathSize == lhsData.getRowCount())
        {
            LOGGER.debug("(Happily) Done!");
            return new ResultTable(keyColumns, results);
        }
        LOGGER.debug("Matched {} rows happily", happyPathSize);
        int firstUnMatchedIndex = happyPathSize;

        LOGGER.debug("Starting Reverse Happy Path (tm)");
        List<List<ResultCell>> reversePathResults = FastList.newList(rhsData.getRowCount() - happyPathSize);
        collectReverseMatchingRows(columnIndices, reversePathResults, rhsData, lhsData, columnComparators, firstUnMatchedIndex);
        int lastUnMatchedOffset = reversePathResults.size();
        LOGGER.debug("Matched {} rows reverse-happily", lastUnMatchedOffset);

        LOGGER.debug("Generating row indices from index " + firstUnMatchedIndex + '.');
        RhsRowIterator rhsRowIterator = new RhsRowIterator(rhsData, columnIndices, columnComparators, firstUnMatchedIndex, lastUnMatchedOffset);
        LhsRowIterator lhsRowIterator = new LhsRowIterator(lhsData, columnIndices, columnComparators, firstUnMatchedIndex, lastUnMatchedOffset);
        IndexMapGenerator<RowView> rowGenerator = new IndexMapGenerator<RowView>(lhsRowIterator, rhsRowIterator, firstUnMatchedIndex);
        rowGenerator.generate();
        MutableList<IndexMap> allMatchedRows = rowGenerator.getMatched();
        LOGGER.debug("Matched a further {} rows using row hashing", allMatchedRows.size());
        MutableList<UnmatchedIndexMap> allMissingRows = rowGenerator.getMissing();
        MutableList<UnmatchedIndexMap> allSurplusRows = rowGenerator.getSurplus();

        MutableList<IndexMap> matchedColumns = columnIndices.select(IndexMap::isMatched);
        LOGGER.debug("Partial-matching {} missing and {} surplus rows", allMissingRows.size(), allSurplusRows.size());
        PartialMatcher partialMatcher = new AdaptivePartialMatcher(rhsData, lhsData, columnComparators, this.bestMatchThreshold);
        if (rhsData instanceof KeyedComparableTable)
        {
            partialMatcher = new KeyColumnPartialMatcher((KeyedComparableTable) rhsData, lhsData, columnComparators, partialMatcher);
        }
        if (this.partialMatchTimeoutMillis > 0)
        {
            partialMatcher = new TimeBoundPartialMatcher(partialMatcher, this.partialMatchTimeoutMillis);
        }
        partialMatcher.match(allMissingRows, allSurplusRows, matchedColumns);

        LOGGER.debug("Merging partial-matches and remaining missing/surplus");
        MutableList<IndexMap> finalRowIndices = allMatchedRows;
        mergePartialMatches(finalRowIndices, allMissingRows, allSurplusRows);

        // todo: fix transitive bug in compareTo() and use finalRowIndices.sortThis()
        finalRowIndices = FastList.newList(new TreeSet<>(finalRowIndices));
        if (this.verifyRowOrder)
        {
            LOGGER.debug("Looking for out of order rows");
            identifyOutOfOrderIndices(finalRowIndices, firstUnMatchedIndex);
        }

        LOGGER.debug("Generating final results");
        buildResults(columnIndices, finalRowIndices, results, reversePathResults, rhsData, lhsData, columnComparators);
        LOGGER.debug("Done");

        return new ResultTable(keyColumns, results);
    }

    private List<List<ResultCell>> toListOfRows(ComparableTable comparableTable, Function2<String, Object, ResultCell> cellFunction)
    {
        List<List<ResultCell>> results = FastList.newList(comparableTable.getRowCount() + 1);
        List<ResultCell> headers = FastList.newList(comparableTable.getColumnCount());
        for (int ci = 0; ci < comparableTable.getColumnCount(); ci++)
        {
            String columnName = comparableTable.getColumnName(ci);
            headers.add(cellFunction.value(columnName, columnName));
        }
        results.add(headers);
        for (int ri = 0; ri < comparableTable.getRowCount(); ri++)
        {
            List<ResultCell> row = FastList.newList(comparableTable.getColumnCount());
            for (int ci = 0; ci < comparableTable.getColumnCount(); ci++)
            {
                String columnName = comparableTable.getColumnName(ci);
                row.add(cellFunction.value(columnName, comparableTable.getValueAt(ri, ci)));
            }
            results.add(row);
        }
        return results;
    }

    private static void verifyHeaders(MutableList<IndexMap> columnIndices, List<List<ResultCell>> results, ComparableTable rhsData, ComparableTable lhsData, CellComparator comparator)
    {
        MutableList<ResultCell> verifiedHeaders = FastList.newList(columnIndices.size());
        for (IndexMap column : columnIndices)
        {
            if (column.isMissing())
            {
                Object lhs = lhsData.getColumnName(column.getLhsIndex());
                verifiedHeaders.add(ResultCell.createMissingCell(comparator.getFormatter(), lhs));
            }
            else if (column.isSurplus())
            {
                Object rhs = rhsData.getColumnName(column.getRhsIndex());
                verifiedHeaders.add(ResultCell.createSurplusCell(comparator.getFormatter(), rhs));
            }
            else
            {
                Object rhs = rhsData.getColumnName(column.getRhsIndex());
                Object lhs = lhsData.getColumnName(column.getLhsIndex());
                ResultCell cell = ResultCell.createMatchedCell(comparator, rhs, lhs);
                if (column.isOutOfOrder())
                {
                    cell = ResultCell.createOutOfOrderCell(comparator.getFormatter(), rhs);
                }
                verifiedHeaders.add(cell);
            }
        }
        results.add(verifiedHeaders);
    }

    private static void collectMatchingRows(MutableList<IndexMap> columnIndices, List<List<ResultCell>> results, ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators)
    {
        int minRowCount = Math.min(rhsData.getRowCount(), lhsData.getRowCount());
        for (int rowIndex = 0; rowIndex < minRowCount; rowIndex++)
        {
            MutableList<ResultCell> row = FastList.newList(columnIndices.size());
            if (!checkRowMatches(columnIndices, results, rhsData, lhsData, columnComparators, rowIndex, rowIndex, row))
            {
                return;
            }
        }
    }

    private static void collectReverseMatchingRows(MutableList<IndexMap> columnIndices, List<List<ResultCell>> reverseHappyPathResults, ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators, int firstUnMatchedIndex)
    {
        int rhsIndex = rhsData.getRowCount() - 1;
        int lhsIndex = lhsData.getRowCount() - 1;
        int minRhsIndex = firstUnMatchedIndex + 1;
        int minLhsIndex = firstUnMatchedIndex + 1;
        while (lhsIndex >= minLhsIndex && rhsIndex >= minRhsIndex)
        {
            MutableList<ResultCell> row = FastList.newList(columnIndices.size());

            if (!checkRowMatches(columnIndices, reverseHappyPathResults, rhsData, lhsData, columnComparators, rhsIndex, lhsIndex, row))
            {
                return;
            }
            lhsIndex--;
            rhsIndex--;
        }
    }

    private static boolean checkRowMatches(MutableList<IndexMap> columnIndices, List<List<ResultCell>> results, ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators, int rhsIndex, int lhsIndex, MutableList<ResultCell> row)
    {
        for (IndexMap column : columnIndices)
        {
            if (column.isMissing())
            {
                CellComparator comparator = columnComparators.getComparator(lhsData.getColumnName(column.getLhsIndex()));
                Object lhs = lhsData.getValueAt(lhsIndex, column.getLhsIndex());
                row.add(ResultCell.createMissingCell(comparator.getFormatter(), lhs));
            }
            else if (column.isSurplus())
            {
                CellComparator comparator = columnComparators.getComparator(rhsData.getColumnName(column.getRhsIndex()));
                Object rhs = rhsData.getValueAt(rhsIndex, column.getRhsIndex());
                row.add(ResultCell.createSurplusCell(comparator.getFormatter(), rhs));
            }
            else
            {
                CellComparator comparator = columnComparators.getComparator(lhsData.getColumnName(column.getLhsIndex()));
                Object rhs = rhsData.getValueAt(rhsIndex, column.getRhsIndex());
                Object lhs = lhsData.getValueAt(lhsIndex, column.getLhsIndex());
                ResultCell cell = ResultCell.createMatchedCell(comparator, rhs, lhs);
                if (cell.isMatch())
                {
                    if (column.isOutOfOrder())
                    {
                        cell = ResultCell.createOutOfOrderCell(comparator.getFormatter(), rhs);
                    }
                    row.add(cell);
                }
                else
                {
                    return false;
                }
            }
        }
        results.add(row);
        return true;
    }

    private static void identifyOutOfOrderIndices(MutableList<IndexMap> indexMaps, int nextLhsIndex)
    {
        IntHashSet lhsIndices = new IntHashSet(indexMaps.size());
        for (IndexMap indexMap : indexMaps)
        {
            if (!indexMap.isSurplus())
            {
                lhsIndices.add(indexMap.getLhsIndex());
            }
        }
        for (IndexMap im : indexMaps)
        {
            if (!im.isSurplus())
            {
                lhsIndices.remove(im.getLhsIndex());
                if (im.getLhsIndex() == nextLhsIndex)
                {
                    while (!lhsIndices.contains(nextLhsIndex) && !lhsIndices.isEmpty())
                    {
                        nextLhsIndex++;
                    }
                }
                else
                {
                    im.setOutOfOrder();
                }
            }
        }
    }

    private void buildResults(MutableList<IndexMap> columnIndices, MutableList<IndexMap> finalRowIndices, List<List<ResultCell>> results, List<List<ResultCell>> reverseResults, ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators)
    {
        for (IndexMap rowIndexMap : finalRowIndices)
        {
            MutableList<ResultCell> row = FastList.newList();
            if (rowIndexMap.isMissing())
            {
                if (!this.ignoreMissingRows)
                {
                    for (IndexMap colIndexMap : columnIndices)
                    {
                        addMissingRecord(rowIndexMap, row, colIndexMap, lhsData, columnComparators);
                    }
                }
            }
            else if (rowIndexMap.isSurplus())
            {
                if (!this.ignoreSurplusRows)
                {
                    for (IndexMap colIndexMap : columnIndices)
                    {
                        addSurplusRecord(rowIndexMap, row, colIndexMap, rhsData, columnComparators);
                    }
                }
            }
            else
            {
                for (IndexMap colIndexMap : columnIndices)
                {
                    addMatchedRecord(rowIndexMap, row, colIndexMap, rhsData, lhsData, columnComparators);
                }
            }

            if (row.notEmpty())
            {
                results.add(row);
            }
        }
        for (int i = reverseResults.size() - 1; i >= 0; i--)
        {
            results.add(reverseResults.get(i));
        }
    }

    private static void addSurplusRecord(IndexMap rowIndexMap, MutableList<ResultCell> row, IndexMap colIndexMap, ComparableTable rhsData, ColumnComparators columnComparators)
    {
        if (rowIndexMap.getRhsIndex() >= 0 && colIndexMap.getRhsIndex() >= 0)
        {
            CellComparator comparator = columnComparators.getComparator(rhsData.getColumnName(colIndexMap.getRhsIndex()));
            Object displayValue = rhsData.getValueAt(rowIndexMap.getRhsIndex(), colIndexMap.getRhsIndex());
            row.add(ResultCell.createSurplusCell(comparator.getFormatter(), displayValue));
        }
        else
        {
            row.add(ResultCell.createSurplusCell(columnComparators.getDefaultComparator().getFormatter(), ""));
        }
    }

    private static void addMissingRecord(IndexMap rowIndexMap, MutableList<ResultCell> row, IndexMap colIndexMap, ComparableTable lhsData, ColumnComparators columnComparators)
    {
        if (rowIndexMap.getLhsIndex() >= 0 && colIndexMap.getLhsIndex() >= 0)
        {
            CellComparator comparator = columnComparators.getComparator(lhsData.getColumnName(colIndexMap.getLhsIndex()));
            Object displayValue = lhsData.getValueAt(rowIndexMap.getLhsIndex(), colIndexMap.getLhsIndex());
            row.add(ResultCell.createMissingCell(comparator.getFormatter(), displayValue));
        }
        else
        {
            row.add(ResultCell.createMissingCell(columnComparators.getDefaultComparator().getFormatter(), ""));
        }
    }

    private static void addMatchedRecord(IndexMap rowIndexMap, MutableList<ResultCell> row, IndexMap colIndexMap, ComparableTable rhsData, ComparableTable lhsData, ColumnComparators columnComparators)
    {
        if (colIndexMap.isMissing())
        {
            addMissingRecord(rowIndexMap, row, colIndexMap, lhsData, columnComparators);
        }
        else if (colIndexMap.isSurplus())
        {
            addSurplusRecord(rowIndexMap, row, colIndexMap, rhsData, columnComparators);
        }
        else
        {
            CellComparator comparator = columnComparators.getComparator(lhsData.getColumnName(colIndexMap.getLhsIndex()));
            Object rhs = rhsData.getValueAt(rowIndexMap.getRhsIndex(), colIndexMap.getRhsIndex());
            Object lhs = lhsData.getValueAt(rowIndexMap.getLhsIndex(), colIndexMap.getLhsIndex());
            ResultCell comparisonResult = ResultCell.createMatchedCell(comparator, rhs, lhs);
            boolean outOfOrder = rowIndexMap.isOutOfOrder() || colIndexMap.isOutOfOrder();
            // todo: modify comparator to handle out-of-order state internally
            if (outOfOrder && comparisonResult.isMatch())
            {
                comparisonResult = ResultCell.createOutOfOrderCell(comparator.getFormatter(), rhs);
            }
            row.add(comparisonResult);
        }
    }

    private static void mergePartialMatches(MutableList<IndexMap> finalRowIndices, MutableList<UnmatchedIndexMap> allMissingRows, MutableList<UnmatchedIndexMap> allSurplusRows)
    {
        MutableSet<IndexMap> partiallyMatchedSurplus = UnifiedSet.newSet();
        for (UnmatchedIndexMap lhs : allMissingRows)
        {
            UnmatchedIndexMap rhs = lhs.getBestMutualMatch();
            if (rhs == null)
            {
                finalRowIndices.add(lhs);
            }
            else
            {
                // todo: can we avoi newing up another index map - update lhs in place?
                finalRowIndices.add(new IndexMap(lhs.getLhsIndex(), rhs.getRhsIndex()));
                partiallyMatchedSurplus.add(rhs);
            }
        }

        for (IndexMap indexMap : allSurplusRows)
        {
            if (!partiallyMatchedSurplus.contains(indexMap))
            {
                finalRowIndices.add(indexMap);
            }
        }
    }

    private MutableList<IndexMap> getColumnIndices(ComparableTable rhsData, ComparableTable lhsData, CellComparator comparator)
    {
        List<String> lhsHeadings = getHeadings(lhsData, comparator);
        List<String> rhsHeadings = getHeadings(rhsData, comparator);
        IndexMapGenerator<String> columnGenerator = new IndexMapGenerator<>(lhsHeadings.iterator(), rhsHeadings.iterator(), 0);
        columnGenerator.generate();
        MutableList<IndexMap> all = columnGenerator.getAll();
        return all.reject(indexMap ->
        {
            if (indexMap.isMissing())
            {
                return ignoreMissingColumns;
            }
            return indexMap.isSurplus() && ignoreSurplusColumns;
        });
    }

    private static List<String> getHeadings(ComparableTable table, CellComparator comparator)
    {
        FastList<String> headings = FastList.newList();
        for (int i = 0; i < table.getColumnCount(); i++)
        {
            headings.add(comparator.getFormatter().format(table.getColumnName(i)));
        }
        return headings;
    }

}
