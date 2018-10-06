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

import org.junit.internal.AssumptionViolatedException;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

public class TestWatcherBridge extends TestWatcher
{
    private final TableVerifier tableVerifier;

    public TestWatcherBridge(TableVerifier tableVerifier)
    {
        this.tableVerifier = tableVerifier;
    }

    @Override
    public final void starting(Description description)
    {
        this.tableVerifier.starting(description);
    }

    @Override
    public final void succeeded(Description description)
    {
        this.tableVerifier.succeeded(description);
    }

    @Override
    public final void failed(Throwable e, Description description)
    {
        this.tableVerifier.failed(e, description);
    }

    @Override
    public final void skipped(AssumptionViolatedException e, Description description)
    {
        this.tableVerifier.skipped(e, description);
    }

    @Override
    public final void finished(Description description)
    {
        this.tableVerifier.finished(description);
    }
}
