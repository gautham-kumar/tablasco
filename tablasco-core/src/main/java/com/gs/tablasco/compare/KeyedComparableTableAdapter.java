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

package com.gs.tablasco.compare;

import com.gs.tablasco.ComparableTable;
import org.eclipse.collections.api.set.primitive.ImmutableIntSet;
import org.eclipse.collections.impl.factory.primitive.IntSets;

public class KeyedComparableTableAdapter extends DefaultComparableTableAdapter implements KeyedComparableTable
{
    private final ImmutableIntSet keyColumnIndices;

    public KeyedComparableTableAdapter(ComparableTable delegate, int... keyColumnIndices)
    {
        super(delegate);
        this.keyColumnIndices = IntSets.immutable.of(keyColumnIndices);
    }

    @Override
    public boolean isKeyColumn(int columnIndex)
    {
        return this.keyColumnIndices.contains(columnIndex);
    }
}
