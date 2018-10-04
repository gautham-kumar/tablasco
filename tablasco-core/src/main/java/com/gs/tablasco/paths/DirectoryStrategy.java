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

/**
 * A strategy for determining the lhs results and verification output directories for a given test class.
 */
public interface DirectoryStrategy
{
    /**
     * Returns the lhs results directory for a given test class.
     * @param testClass the test class
     * @return the lhs results directory
     */
    Path getLhsDirectory(Class<?> testClass);

    /**
     * Returns the verification output directory for a given test class.
     * @param testClass the test class
     * @return the verification output directory
     */
    Path getOutputDirectory(Class<?> testClass);

    /**
     * Returns the rhs results directory for a given test class.
     * @param testClass the test class
     * @return the verification output directory
     */
    Path getRhsDirectory(Class<?> testClass);
}
