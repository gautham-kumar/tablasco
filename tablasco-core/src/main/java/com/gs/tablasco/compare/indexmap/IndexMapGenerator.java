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

package com.gs.tablasco.compare.indexmap;

import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.list.mutable.FastList;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class IndexMapGenerator<T>
{
    private final Iterator<T> rhsIterator;
    private final Iterator<T> lhsIterator;
    private MutableList<IndexMap> matched;
    private MutableList<UnmatchedIndexMap> missing;
    private MutableList<UnmatchedIndexMap> surplus;
    private final int initialIndex;

    public IndexMapGenerator(Iterator<T> lhsIterator, Iterator<T> rhsIterator, int initialIndex)
    {
        this.rhsIterator = rhsIterator;
        this.lhsIterator = lhsIterator;
        this.initialIndex = initialIndex;
    }

    public MutableList<IndexMap> getMatched()
    {
        return this.matched;
    }

    public MutableList<UnmatchedIndexMap> getMissing()
    {
        return this.missing;
    }

    public MutableList<UnmatchedIndexMap> getSurplus()
    {
        return this.surplus;
    }

    public MutableList<IndexMap> getAll()
    {
        Set<IndexMap> all = new TreeSet<>();
        all.addAll(this.matched);
        all.addAll(this.surplus);
        all.addAll(this.missing);
        return FastList.newList(all);
    }

    public void generate()
    {
        this.matched = FastList.newList();
        this.missing = FastList.newList();
        this.surplus = FastList.newList();

        Map<T, Object> rhsIndices = new LinkedHashMap<T, Object>();
        int ai = this.initialIndex;

        while (this.rhsIterator.hasNext())
        {
            T next = this.rhsIterator.next();
            Object indexOrListOf = rhsIndices.get(next);
            if (indexOrListOf == null)
            {
                rhsIndices.put(next, Integer.valueOf(ai));
            }
            else if (indexOrListOf instanceof Integer)
            {
                rhsIndices.put(next, FastList.newListWith((Integer) indexOrListOf, Integer.valueOf(ai)));
            }
            else
            {
                ((List) indexOrListOf).add(Integer.valueOf(ai));
            }
            ai++;
        }
        int ei = this.initialIndex;
        while (this.lhsIterator.hasNext())
        {
            T next = this.lhsIterator.next();
            Object rhsIndexOrListOf = rhsIndices.remove(next);
            if (rhsIndexOrListOf == null)
            {
                this.missing.add(new UnmatchedIndexMap(ei, -1));
            }
            else if (rhsIndexOrListOf instanceof Integer)
            {
                this.matched.add(new IndexMap(ei, (Integer) rhsIndexOrListOf));
            }
            else
            {
                List indices = (List) rhsIndexOrListOf;
                Integer rhsIndex = (Integer) indices.remove(0);
                this.matched.add(new IndexMap(ei, rhsIndex));
                rhsIndices.put(next, indices.size() == 1 ? indices.get(0) : indices);
            }
            ei++;
        }
        for (Map.Entry<T, Object> rhsEntry : rhsIndices.entrySet())
        {
            Object indexOrListOf = rhsEntry.getValue();
            if (indexOrListOf instanceof Integer)
            {
                this.surplus.add(new UnmatchedIndexMap(-1, (Integer) indexOrListOf));
            }
            else
            {
                for (Object index : (List) indexOrListOf)
                {
                    this.surplus.add(new UnmatchedIndexMap(-1, (Integer) index));
                }
            }
        }
    }
}

