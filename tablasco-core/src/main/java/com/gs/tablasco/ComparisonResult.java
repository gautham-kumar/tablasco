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
import com.gs.tablasco.compare.HtmlFormatter;
import com.gs.tablasco.compare.Metadata;
import org.eclipse.collections.impl.factory.Maps;

import java.nio.file.Path;
import java.util.Map;

public class ComparisonResult
{
    private final FormattableTable formattableTable;
    private final int compareCount;
    private final HtmlFormatter htmlFormatter;

    public ComparisonResult(FormattableTable formattableTable, int compareCount, HtmlFormatter htmlFormatter)
    {
        this.formattableTable = formattableTable;
        this.compareCount = compareCount;
        this.htmlFormatter = htmlFormatter;
    }

    public boolean isSuccess()
    {
        return this.formattableTable.isSuccess();
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
        Map<String, FormattableTable> results = Maps.fixedSize.of(this.formattableTable.getTableName(), this.formattableTable);
        this.htmlFormatter.appendResults(outputPath, comparisonName, results, metadata, this.compareCount);
    }
}
