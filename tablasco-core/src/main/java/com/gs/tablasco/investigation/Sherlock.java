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

import com.gs.tablasco.compare.ResultTable;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.impl.utility.Iterate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Compares results from two environments and drills down on breaks in multiple
 * steps until it finds the underlying data responsible for the breaks.
 */
public class Sherlock
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Sherlock.class);

    public void handle(Investigation investigation, Path outputPath, Procedure2<String, Map<String, ResultTable>> appendToHtml)
    {
        Watson watson = new Watson(appendToHtml);
        InvestigationLevel currentLevel = investigation.getFirstLevel();
        List<Object> drilldownKeys = watson.assist("Initial Results", currentLevel, investigation.getRowKeyLimit());
        if (Iterate.isEmpty(drilldownKeys))
        {
            LOGGER.info("No breaks found :)");
            return;
        }

        LOGGER.info("Got " + drilldownKeys.size() + " broken drilldown keys - " + outputPath);
        int level = 1;
        while (!drilldownKeys.isEmpty() && (currentLevel = investigation.getNextLevel(drilldownKeys)) != null)
        {
            drilldownKeys = watson.assist("Investigation Level " + level + " (Top " + investigation.getRowKeyLimit() + ')', currentLevel, investigation.getRowKeyLimit());
            LOGGER.info("Got " + drilldownKeys.size() + " broken drilldown keys - " + outputPath);
            level++;
        }

        String message = "Some tests failed. Check test results file " + outputPath.toAbsolutePath() + " for more details.";
        throw new AssertionError(message);
    }
}
