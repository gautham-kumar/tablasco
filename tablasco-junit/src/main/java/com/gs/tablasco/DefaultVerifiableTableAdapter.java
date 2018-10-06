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

import com.gs.tablasco.compare.DefaultComparableTableAdapter;

/**
 * A default <tt>VerifiableTable</tt> adapter that delegates all calls to an underlying delegate table. Extend this
 * class if you only need to modify behaviour of some methods of the underlying table.
 */
public class DefaultVerifiableTableAdapter extends DefaultComparableTableAdapter implements VerifiableTable
{
    public DefaultVerifiableTableAdapter(ComparableTable delegate)
    {
        super(delegate);
    }

    public DefaultVerifiableTableAdapter(String name, ComparableTable delegate)
    {
        super(name, delegate);
    }
}
