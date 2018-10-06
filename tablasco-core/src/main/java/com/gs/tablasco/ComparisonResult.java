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
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ListIterate;

import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;

public class ComparisonResult
{
    private final List<Pair<String, FormattableTable>> formattableTables;
    private final HtmlFormatter htmlFormatter;

    public ComparisonResult(String name, FormattableTable formattableTable, HtmlFormatter htmlFormatter)
    {
        this(Lists.fixedSize.of(Tuples.pair(name, formattableTable)), htmlFormatter);
    }

    public ComparisonResult(List<Pair<String, FormattableTable>> formattableTables, HtmlFormatter htmlFormatter)
    {
        this.formattableTables = formattableTables;
        this.htmlFormatter = htmlFormatter;
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
        LinkedHashMap<String, FormattableTable> map = new LinkedHashMap<>();
        this.formattableTables.forEach(pair -> map.put(pair.getOne(), pair.getTwo()));
        this.htmlFormatter.appendResults(outputPath, comparisonName, map, metadata, compareCount);
    }

    public static ComparisonResult newEmpty(HtmlFormatter htmlFormatter)
    {
        return new ComparisonResult(Lists.fixedSize.of(), htmlFormatter);
    }

    public ComparisonResult combine(ComparisonResult result)
    {
        List<Pair<String, FormattableTable>> allTables = Lists.fixedSize.ofAll(this.formattableTables).withAll(result.formattableTables);
        return new ComparisonResult(allTables, result.htmlFormatter);
    }
}
