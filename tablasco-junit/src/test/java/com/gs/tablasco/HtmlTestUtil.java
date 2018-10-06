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
import com.gs.tablasco.compare.ResultTable;
import com.gs.tablasco.compare.SummaryResultTable;
import org.eclipse.collections.impl.factory.Maps;

import java.nio.file.Path;
import java.util.Map;

public class HtmlTestUtil
{
    public static void append(Path path, String methodName, String tableName, ResultTable resultTable)
    {
        append(path, methodName, Maps.fixedSize.of(tableName, new SummaryResultTable(resultTable)));
    }

    public static void append(Path path, String methodName, Map<String, ? extends FormattableTable> results)
    {
        HtmlFormatter htmlFormatter = new HtmlFormatter(
                new HtmlOptions(false, HtmlFormatter.DEFAULT_ROW_LIMIT, false, false, false));

        htmlFormatter.appendResults(path, methodName, results, Metadata.newEmpty());
    }
}
