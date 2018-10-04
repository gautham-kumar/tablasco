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

/**
 * A strategy for determining the lhs results and comparison output pathnames
 */
public interface PathNameStrategy
{
    /**
     * Returns the lhs results pathname for a given test class.
     * @param testClass the test class
     * @return the lhs results pathname
     */
    String getLhsPathname(Class<?> testClass, String methodName);

    /**
     * Returns the comparison output pathname for a given test class.
     * @param testClass the test class
     * @return the comparison output pathname
     */
    String getOutputPathname(Class<?> testClass, String methodName);

    /**
     * Returns the rhs results pathname for a given test class.
     * @param testClass the test class
     * @return the comparison output pathname
     */
    String getRhsPathname(Class<?> testClass, String methodName);
}
