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

import com.gs.tablasco.compare.FormattableTable;
import com.gs.tablasco.compare.Metadata;
import com.gs.tablasco.compare.SummaryResultTable;
import org.eclipse.collections.api.partition.list.PartitionMutableList;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.block.factory.Predicates;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;

import javax.xml.transform.TransformerException;
import java.io.OutputStream;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ComparisonResult implements Serializable
{
    private static final Predicates<Pair<String, FormattableTable>> IS_SUMMARIZED =
            Predicates.attributePredicate(Functions.secondOfPair(), Predicates.instanceOf(SummaryResultTable.class));

    private final List<Pair<String, FormattableTable>> formattableTables;
    private final HtmlOptions htmlOptions;

    public ComparisonResult(String name, FormattableTable formattableTable, HtmlOptions htmlOptions)
    {
        this(Lists.fixedSize.of(Tuples.pair(name, formattableTable)), htmlOptions);
    }

    public ComparisonResult(List<Pair<String, FormattableTable>> formattableTables, HtmlOptions htmlOptions)
    {
        this.formattableTables = formattableTables;
        this.htmlOptions = htmlOptions;
    }

    public boolean isSuccess()
    {
        return ListIterate.collect(this.formattableTables, Functions.secondOfPair()).allSatisfy(FormattableTable::isSuccess);
    }

    public void generateBreakReport(Path outputPath, int compareCount)
    {
        this.generateBreakReport("Break Report", outputPath, Metadata.newEmpty(), compareCount);
    }

    public void generateBreakReport(String comparisonName, Path outputPath, int compareCount)
    {
        this.generateBreakReport(comparisonName, outputPath, Metadata.newEmpty(), compareCount);
    }

    public void generateBreakReport(String comparisonName, Path outputPath, Metadata metadata, int compareCount)
    {
        new HtmlFormatter(this.htmlOptions).appendResults(outputPath, comparisonName, this.getOrderedTables(), metadata, compareCount);
    }

    public void writeBreakReportToStream(String comparisonName, Metadata metadata, OutputStream stream) throws TransformerException, UnsupportedEncodingException
    {
        new HtmlFormatter(this.htmlOptions).appendResults(comparisonName, this.getOrderedTables(), metadata, 1, null, stream);
    }

    private Map<String, FormattableTable> getOrderedTables()
    {
        LinkedHashMap<String, FormattableTable> map = new LinkedHashMap<>();
        this.formattableTables.forEach(pair -> map.put(pair.getOne(), pair.getTwo()));
        return map;
    }

    public static ComparisonResult newEmpty(HtmlOptions htmlOptions)
    {
        return new ComparisonResult(Lists.fixedSize.of(), htmlOptions);
    }

    public ComparisonResult combine(ComparisonResult result, boolean mergeSummarizedResults)
    {
        return new ComparisonResult(this.combineTables(result, mergeSummarizedResults), result.htmlOptions);
    }

    private List<Pair<String, FormattableTable>> combineTables(ComparisonResult result, boolean mergeSummarizedResults)
    {
        List<Pair<String, FormattableTable>> allTables = Lists.fixedSize.ofAll(this.formattableTables).withAll(result.formattableTables);

        if (mergeSummarizedResults)
        {
            PartitionMutableList<Pair<String, FormattableTable>> partitioned = ListIterate.partition(allTables, IS_SUMMARIZED);

            Optional<Pair<String, FormattableTable>> merged = partitioned
                    .getSelected()
                    .stream()
                    .reduce((existing, incoming) -> Tuples.pair("Summary", mergeSummarizedResults(existing, incoming)));

            return merged.map(pair -> partitioned.getRejected().with(pair)).orElseGet(partitioned::getRejected);
        }

        return allTables;
    }

    private static SummaryResultTable mergeSummarizedResults(Pair<String, FormattableTable> existing, Pair<String, FormattableTable> incoming)
    {
        return SummaryResultTable.class.cast(existing.getTwo()).merge(SummaryResultTable.class.cast(incoming.getTwo()));
    }
}
