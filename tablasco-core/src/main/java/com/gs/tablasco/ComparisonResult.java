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
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.block.factory.Functions;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;

import java.nio.file.Path;
import java.util.List;

public class ComparisonResult
{
    private final List<Pair<String, FormattableTable>> formattableTables;
    private final int compareCount;
    private final HtmlFormatter htmlFormatter;

    public ComparisonResult(String name, FormattableTable formattableTable, int compareCount, HtmlFormatter htmlFormatter)
    {
        this(Lists.fixedSize.of(Tuples.pair(name, formattableTable)), compareCount, htmlFormatter);
    }

    public ComparisonResult(List<Pair<String, FormattableTable>> formattableTables, int compareCount, HtmlFormatter htmlFormatter)
    {
        this.formattableTables = formattableTables;
        this.compareCount = compareCount;
        this.htmlFormatter = htmlFormatter;
    }

    public boolean isSuccess()
    {
        return ListIterate.collect(this.formattableTables, Functions.secondOfPair()).allSatisfy(FormattableTable::isSuccess);
    }

    public void generateBreakReport(Path outputPath)
    {
        this.generateBreakReport("Break Report", outputPath, Metadata.newEmpty());
    }

    public void generateBreakReport(String comparisonName, Path outputPath)
    {
        this.generateBreakReport(comparisonName, outputPath, Metadata.newEmpty());
    }

    public void generateBreakReport(String comparisonName, Path outputPath, Metadata metadata)
    {
        this.htmlFormatter.appendResults(outputPath, comparisonName, UnifiedMap.newMapWith(this.formattableTables), metadata, this.compareCount);
    }

    public static ComparisonResult newEmpty(HtmlFormatter htmlFormatter)
    {
        return new ComparisonResult(Lists.fixedSize.of(), 0, htmlFormatter);
    }

    public ComparisonResult combine(ComparisonResult result)
    {
        List<Pair<String, FormattableTable>> allTables = Lists.fixedSize.ofAll(this.formattableTables).withAll(result.formattableTables);
        return new ComparisonResult(allTables, this.compareCount + result.compareCount, result.htmlFormatter);
    }
}
