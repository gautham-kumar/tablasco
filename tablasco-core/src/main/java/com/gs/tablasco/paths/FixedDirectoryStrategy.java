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

package com.gs.tablasco.paths;

import java.nio.file.Path;
import java.nio.file.Paths;

public class FixedDirectoryStrategy implements DirectoryStrategy
{
    private final Path lhsDir;
    private final Path outputDir;

    public FixedDirectoryStrategy()
    {
        this(Paths.get(System.getProperty("user.dir")), Paths.get(System.getProperty("java.io.tmpdir")));
    }

    public FixedDirectoryStrategy(Path lhsDir, Path outputDir)
    {
        this.lhsDir = lhsDir;
        this.outputDir = outputDir;
    }

    @Override
    public Path getLhsDirectory(Class<?> testClass)
    {
        return this.lhsDir;
    }

    @Override
    public Path getOutputDirectory(Class<?> testClass)
    {
        return this.outputDir;
    }

    @Override
    public Path getRhsDirectory(Class<?> testClass)
    {
        return this.outputDir;
    }
}
