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

package com.gs.tablasco;


import com.gs.tablasco.adapters.TableAdapters;
import com.gs.tablasco.compare.*;
import com.gs.tablasco.compare.indexmap.IndexMapTableComparator;
import com.gs.tablasco.investigation.Investigation;
import com.gs.tablasco.investigation.Sherlock;
import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.predicate.Predicate;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Sets;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

/**
 * A Utility that can be used for comparing tabular data represented as instances of <tt>ComparableTable</tt>.
 * The comparison results are published as a HTML file in the specified output directory.
 * <p>
 * A number of configuration options are available to influence the behaviour of <tt>TableComparator</tt>. A fluent
 * interface allows configuration options to be combined in a flexible manner.
 * <p>
 */
public class TableComparator<T extends TableComparator<T>>
{
    private boolean compareRowOrder = true;
    private boolean hideMatchedRows = false;
    private boolean hideMatchedTables = false;
    private boolean hideMatchedColumns = false;
    private boolean assertionSummary = false;
    private boolean ignoreSurplusRows = false;
    private boolean ignoreMissingRows = false;
    private boolean ignoreSurplusColumns = false;
    private boolean ignoreMissingColumns = false;
    private Function<ComparableTable, ComparableTable> rhsAdapter = Functions.getPassThru();
    private Function<ComparableTable, ComparableTable> lhsAdapter = Functions.getPassThru();
    private long partialMatchTimeoutMillis = IndexMapTableComparator.DEFAULT_PARTIAL_MATCH_TIMEOUT_MILLIS;

    private int htmlRowLimit = HtmlFormatter.DEFAULT_ROW_LIMIT;
    private boolean summarisedResults = false;

    private final ColumnComparators.Builder columnComparatorsBuilder = new ColumnComparators.Builder();

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with row order comparison disabled. If this is
     * disabled a check will pass if the cells match but row order is different between rhs and lhs results.
     *
     * @param compareRowOrder whether to compare row order or not
     * @return this
     */
    public final T withCompareRowOrder(boolean compareRowOrder)
    {
        this.compareRowOrder = compareRowOrder;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a numeric tolerance to apply when matching
     * floating point numbers.
     * <p>
     * Note: this tolerance applies to all floating-point column types which could be dangerous. It is generally
     * advisable to set tolerance per column using {@link #withTolerance(String, double) withTolerance}
     *
     * @param tolerance the tolerance to apply
     * @return this
     */
    public final T withTolerance(double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(tolerance);
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a numeric tolerance to apply when matching
     * floating point numbers for the given column.
     *
     * @param columnName the column name for which the tolerance will be applied
     * @param tolerance  the tolerance to apply
     * @return this
     */
    public final T withTolerance(String columnName, double tolerance)
    {
        this.columnComparatorsBuilder.withTolerance(columnName, tolerance);
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a variance threshold to apply when matching
     * numbers.
     * <p>
     * Note: this variance threshold applies to all floating-point column types which could be dangerous. It is
     * generally advisable to set variance threshold per column using {@link #withVarianceThreshold(String, double)
     * withVarianceThreshold}
     *
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public final T withVarianceThreshold(double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(varianceThreshold);
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a variance threshold to apply when matching
     * numbers for the given column.
     *
     * @param columnName        the column name for which the variance will be applied
     * @param varianceThreshold the variance threshold to apply
     * @return this
     */
    public final T withVarianceThreshold(String columnName, double varianceThreshold)
    {
        this.columnComparatorsBuilder.withVarianceThreshold(columnName, varianceThreshold);
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to exclude matched rows from the comparison
     * output.
     *
     * @param hideMatchedRows whether to hide matched rows or not
     * @return this
     */
    public final T withHideMatchedRows(boolean hideMatchedRows)
    {
        this.hideMatchedRows = hideMatchedRows;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to exclude matched columns from the comparison
     * output.
     *
     * @param hideMatchedColumns whether to hide matched columns or not
     * @return this
     */
    public final T withHideMatchedColumns(boolean hideMatchedColumns)
    {
        this.hideMatchedColumns = hideMatchedColumns;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to exclude matched tables from the comparison
     * output. If this is enabled and all tables are matched not output file will be created.
     *
     * @param hideMatchedTables whether to hide matched tables or not
     * @return this
     */
    public final T withHideMatchedTables(boolean hideMatchedTables)
    {
        this.hideMatchedTables = hideMatchedTables;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to limit the number of HTML rows to the specified
     * number.
     *
     * @param htmlRowLimit the number of rows to limit output to
     * @return this
     */
    public final T withHtmlRowLimit(int htmlRowLimit)
    {
        this.htmlRowLimit = htmlRowLimit;
        return self();
    }

    /**
     * Adds an assertion summary to html output
     *
     * @return this
     */
    public final T withAssertionSummary(boolean assertionSummary)
    {
        this.assertionSummary = assertionSummary;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a function for adapting rhs results. Each
     * table in the rhs results will be adapted using the specified function before being verified or rebased.
     *
     * @param rhsAdapter function for adapting tables
     * @return this
     */
    public final T withRhsAdapter(Function<ComparableTable, ComparableTable> rhsAdapter)
    {
        this.rhsAdapter = sanitizeAdapter(rhsAdapter);
        return self();
    }

    /**
     * Returns the rhs table adapter
     *
     * @return - the rhs table adapter
     */
    public Function<ComparableTable, ComparableTable> getRhsAdapter()
    {
        return sanitizeAdapter(this.rhsAdapter);
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with a function for adapting lhs results.
     * Each table in the lhs results will be adapted using the specified function before being verified.
     *
     * @param lhsAdapter function for adapting tables
     * @return this
     */
    public final T withLhsAdapter(Function<ComparableTable, ComparableTable> lhsAdapter)
    {
        this.lhsAdapter = lhsAdapter;
        return self();
    }

    /**
     * Returns the lhs table adapter
     *
     * @return - the lhs table adapter
     */
    public Function<ComparableTable, ComparableTable> getLhsAdapter()
    {
        return this.lhsAdapter;
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to ignore surplus rows from the comparison.
     *
     * @return this
     */
    public final T withIgnoreSurplusRows()
    {
        this.ignoreSurplusRows = true;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to ignore missing rows from the comparison.
     *
     * @return this
     */
    public final T withIgnoreMissingRows()
    {
        this.ignoreMissingRows = true;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to ignore surplus columns from the comparison.
     *
     * @return this
     */
    public final T withIgnoreSurplusColumns()
    {
        this.ignoreSurplusColumns = true;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to ignore missing columns from the comparison.
     *
     * @return this
     */
    public final T withIgnoreMissingColumns()
    {
        this.ignoreMissingColumns = true;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to ignore columns from both the rhs and
     * lhs tables.
     *
     * @param columnsToIgnore the columns to ignore
     * @return this
     */
    public final T withIgnoreColumns(String... columnsToIgnore)
    {
        Set<String> columnSet = Sets.immutable.of(columnsToIgnore).castToSet();
        return this.withColumnFilter(s -> !columnSet.contains(s));
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to filter columns from both the rhs and
     * lhs tables.
     *
     * @param columnFilter the column filter to apply
     * @return this
     */
    public final T withColumnFilter(final Predicate<String> columnFilter)
    {
        Function<ComparableTable, ComparableTable> adapter = table -> TableAdapters.withColumns(table, columnFilter);
        return this.withRhsAdapter(adapter).withLhsAdapter(adapter);
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured to format HTML output grouped and summarised by
     * break type.
     *
     * @param summarisedResults whether to summarise results or not
     * @return this
     */
    public final T withSummarisedResults(boolean summarisedResults)
    {
        this.summarisedResults = summarisedResults;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with the specified partial match timeout. A value
     * of zero or less results in no timeout.
     *
     * @param partialMatchTimeoutMillis comparison timeout in milliseconds
     * @return this
     */
    public final T withPartialMatchTimeoutMillis(long partialMatchTimeoutMillis)
    {
        this.partialMatchTimeoutMillis = partialMatchTimeoutMillis;
        return self();
    }

    /**
     * Returns the same instance of <tt>TableComparator</tt> configured with no partial match timeout.
     *
     * @return this
     */
    public final T withoutPartialMatchTimeout()
    {
        return this.withPartialMatchTimeoutMillis(0);
    }

    /**
     * Compares two tables.
     *
     * @param lhsTable - lhs table
     * @param rhsTable - rhs table
     */
    public final ComparisonResult compare(ComparableTable lhsTable, ComparableTable rhsTable)
    {
        return this.compare(lhsTable, rhsTable, false, false);
    }

    protected final ComparisonResult compare(ComparableTable lhsTable, ComparableTable rhsTable, boolean skipLhsAdaptation, boolean skipRhsAdaptation)
    {
        ComparableTable adaptedLhsTable = skipLhsAdaptation ? lhsTable : this.lhsAdapter.valueOf(lhsTable);
        ComparableTable adaptedRhsTable = skipRhsAdaptation ? rhsTable : this.rhsAdapter.valueOf(rhsTable);
        FormattableTable resultTable = getVerifiedResults(adaptedLhsTable, adaptedRhsTable);
        return new ComparisonResult(getComparisonName(lhsTable, rhsTable), resultTable, newHtmlFormatter());
    }

    /**
     * Compares results from two environments and drills down on breaks in multiple
     * steps until it finds the underlying data responsible for the breaks.
     */
    public void investigate(Investigation investigation, Path outputPath)
    {
        Procedure2<String, Map<String, ResultTable>> appendToHtml = (levelName, map) ->
        {
            HtmlFormatter formatter = new HtmlFormatter(new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, true, false, Sets.fixedSize.of()));
            formatter.appendResults(outputPath, levelName, map, Metadata.newEmpty());
        };

        new Sherlock().handle(investigation, outputPath, appendToHtml);
    }

    private FormattableTable getVerifiedResults(ComparableTable adaptedLhsTable, ComparableTable adaptedRhsTable)
    {
        if (adaptedRhsTable != null && adaptedRhsTable.getColumnCount() == 0)
        {
            throw new IllegalStateException("RHS table '" + adaptedRhsTable.getTableName() + "' has no columns");
        }
        if (adaptedLhsTable != null && adaptedLhsTable.getColumnCount() == 0)
        {
            throw new IllegalStateException("LHS table '" + adaptedLhsTable.getTableName() + "' has no columns");
        }

        ResultTable resultTable = newSingleTableComparator().compare(adaptedRhsTable, adaptedLhsTable);

        return this.summarisedResults ? new SummaryResultTable(resultTable) : resultTable;
    }

    final HtmlFormatter newHtmlFormatter()
    {
        return new HtmlFormatter(this.getHtmlOptions(Sets.fixedSize.of()));
    }

    protected HtmlOptions getHtmlOptions(Set<String> tablesToAlwaysShowMatchedRowsFor)
    {
        return new HtmlOptions(this.assertionSummary, this.htmlRowLimit, this.hideMatchedTables, this.hideMatchedRows, this.hideMatchedColumns, tablesToAlwaysShowMatchedRowsFor);
    }

    private SingleTableComparator newSingleTableComparator()
    {
        ColumnComparators comparators = this.getColumnComparatorsBuilder().build();
        return new IndexMapTableComparator(comparators, this.compareRowOrder, IndexMapTableComparator.DEFAULT_BEST_MATCH_THRESHOLD, this.ignoreSurplusRows, this.ignoreMissingRows, this.ignoreSurplusColumns, this.ignoreMissingColumns, this.partialMatchTimeoutMillis);
    }

    private String getComparisonName(ComparableTable lhsTable, ComparableTable rhsTable)
    {
        if (lhsTable == null)
        {
            return rhsTable.getTableName();
        }
        if (rhsTable == null)
        {
            return lhsTable.getTableName();
        }
        return Sets.fixedSize.of(lhsTable.getTableName(), rhsTable.getTableName()).makeString();
    }

    protected Function<ComparableTable, ComparableTable> sanitizeAdapter(Function<ComparableTable, ComparableTable> adapter)
    {
        return adapter;
    }

    protected ColumnComparators.Builder getColumnComparatorsBuilder()
    {
        return this.columnComparatorsBuilder;
    }

    T self()
    {
        //noinspection unchecked
        return (T) this;
    }
}
