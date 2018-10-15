package com.gs.tablasco.spark;

import com.gs.tablasco.ComparableTable;
import com.gs.tablasco.ComparisonResult;
import com.gs.tablasco.TableComparator;
import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.compare.ColumnComparators;
import com.gs.tablasco.compare.DefaultComparableTableAdapter;
import com.gs.tablasco.compare.KeyedComparableTable;
import com.gs.tablasco.compare.ListComparableTable;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class VerifyGroupFunction implements Function<Tuple2<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>>, ComparisonResult>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(VerifyGroupFunction.class);

    private final Set<String> groupKeyColumns;
    private final List<String> actualHeaders;
    private final List<String> expectedHeaders;
    private final boolean ignoreSurplusColumns;
    private final Set<String> columnsToIgnore;
    private final Optional<Double> tolerance;
    private final Map<String, Double> columnSpecificTolerance;

    VerifyGroupFunction(Set<String> groupKeyColumns, List<String> actualHeaders, List<String> expectedHeaders,
                        boolean ignoreSurplusColumns, Set<String> columnsToIgnore, Optional<Double> tolerance, Map<String, Double> columnSpecificTolerance)
    {
        this.groupKeyColumns = groupKeyColumns;
        this.actualHeaders = actualHeaders;
        this.expectedHeaders = expectedHeaders;
        this.ignoreSurplusColumns = ignoreSurplusColumns;
        this.tolerance = tolerance;
        this.columnsToIgnore = columnsToIgnore;
        this.columnSpecificTolerance = columnSpecificTolerance;
    }

    @Override
    public ComparisonResult call(Tuple2<Integer, Tuple2<Optional<Iterable<List<Object>>>, Optional<Iterable<List<Object>>>>> v1)
    {
        Integer shardNumber = v1._1();

        Optional<Iterable<List<Object>>> actualOptional = v1._2()._1();
        Optional<Iterable<List<Object>>> expectedOptional = v1._2()._2();

        Iterable<List<Object>> actualRows = actualOptional.isPresent() ? actualOptional.get() : Collections.emptyList();
        Iterable<List<Object>> expectedRows = expectedOptional.isPresent() ? expectedOptional.get() : Collections.emptyList();

        ComparableTable actualTable = this.getVerifiableTable(actualRows, this.actualHeaders);
        ComparableTable expectedTable = this.getVerifiableTable(expectedRows, this.expectedHeaders);

        ComparisonResult comparisonResult = this.constructTableComparator().compare(expectedTable, actualTable);

        LOGGER.info("Verification of shard {} {}", shardNumber, comparisonResult.isSuccess() ? "PASSED" : "FAILED");

        return comparisonResult;
    }

    private TableComparator constructTableComparator()
    {
        TableComparator comparator = new SparkTableComparator()
                .withCompareRowOrder(false)
                .withSummarisedResults(true)
                .withoutPartialMatchTimeout();

        if (this.ignoreSurplusColumns)
        {
            comparator = comparator.withIgnoreSurplusColumns();
        }

        if (this.tolerance.isPresent())
        {
            comparator = comparator.withTolerance(this.tolerance.get());
        }

        for (Map.Entry<String, Double> entry : this.columnSpecificTolerance.entrySet())
        {
            comparator = comparator.withTolerance(entry.getKey(), entry.getValue());
        }

        return comparator;
    }

    private ComparableTable getVerifiableTable(Iterable<List<Object>> data, List<String> headers)
    {
        ComparableTable verifiableTable = new ListComparableTable("Summary", headers, FastList.newList(data));
        if (this.columnsToIgnore != null)
        {
            verifiableTable = TableAdapters.withColumns(verifiableTable, s -> !this.columnsToIgnore.contains(s));
        }
        return this.groupKeyColumns.isEmpty() ? verifiableTable : new GroupKeyedVerifiableTable(verifiableTable, this.groupKeyColumns);
    }

    private static class GroupKeyedVerifiableTable extends DefaultComparableTableAdapter implements KeyedComparableTable
    {
        private final Set<String> groupKeyColumns;

        GroupKeyedVerifiableTable(ComparableTable delegate, Set<String> groupKeyColumns)
        {
            super(delegate);
            this.groupKeyColumns = groupKeyColumns;
        }

        @Override
        public boolean isKeyColumn(int columnIndex)
        {
            return columnIndex >= 0 && this.groupKeyColumns.contains(this.getColumnName(columnIndex));
        }
    }

    private static class SparkTableComparator extends TableComparator
    {
        @Override
        protected ColumnComparators.Builder getColumnComparatorsBuilder()
        {
            return super.getColumnComparatorsBuilder().withLabels("Expected", "Actual");
        }
    }
}
