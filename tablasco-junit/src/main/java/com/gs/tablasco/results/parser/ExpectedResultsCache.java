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

package com.gs.tablasco.results.parser;

import com.gs.tablasco.results.ParsedResults;
import com.gs.tablasco.results.TableDataLoader;

import java.io.File;
import java.util.Map;
import java.util.WeakHashMap;

public class ExpectedResultsCache
{
    private static final Map<File, ParsedResults> RESULT_CACHES = new WeakHashMap<>();

    public static ParsedResults getExpectedResults(TableDataLoader expectedResultsLoader, File expectedResultsFile)
    {
        ParsedResults cached = RESULT_CACHES.get(expectedResultsFile);
        if (cached != null)
        {
            return cached;
        }
        ParsedResults expectedResults = new TableDataParser(expectedResultsLoader, expectedResultsFile).parse();
        RESULT_CACHES.put(expectedResultsFile, expectedResults);
        return expectedResults;
    }
}
