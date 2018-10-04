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

import org.eclipse.collections.api.block.SerializableComparator;
import org.eclipse.collections.api.block.function.primitive.IntFunction;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.block.factory.Comparators;

import java.util.SortedSet;
import java.util.TreeSet;

public class UnmatchedIndexMap extends IndexMap
{
    private SortedSet<Match> partialMatches;
    private UnmatchedIndexMap bestMutualMatch;

    public UnmatchedIndexMap(int lhsIndex, int rhsIndex)
    {
        super(lhsIndex, rhsIndex);
    }

    static void linkBestMatches(MutableList<UnmatchedIndexMap> allMissingRows)
    {
        boolean keepMatching = true;
        while (keepMatching)
        {
            keepMatching = false;
            for (UnmatchedIndexMap lhs : allMissingRows)
            {
                keepMatching |= lhs.match();
            }
        }
    }

    public void addMatch(int matchScore, UnmatchedIndexMap match)
    {
        if (this.equals(match))
        {
            throw new IllegalArgumentException("Cannot add this as partial match");
        }
        if (this.partialMatches == null)
        {
            this.partialMatches = new TreeSet<>();
        }
        if (match.partialMatches == null)
        {
            match.partialMatches = new TreeSet<>();
        }
        if (this.getLhsIndex() < 0 || match.getRhsIndex() < 0)
        {
            throw new IllegalStateException("Expecting this to be lhs and that to be rhs");
        }

        this.partialMatches.add(new Match(matchScore, match));
        match.partialMatches.add(new Match(matchScore, this));
    }

    public boolean match()
    {
        UnmatchedIndexMap thisBest = this.getBestMatch();
        if (thisBest != null)
        {
            UnmatchedIndexMap thatBest = thisBest.getBestMatch();
            if (this.equals(thatBest))
            {
                this.bestMutualMatch = thisBest;
                this.partialMatches = null;
                thisBest.bestMutualMatch = this;
                thisBest.partialMatches = null;
                return true;
            }
        }
        return false;
    }

    private UnmatchedIndexMap getBestMatch()
    {
        if (this.partialMatches != null)
        {
            for (Match match : this.partialMatches)
            {
                if (match.match.bestMutualMatch == null)
                {
                    return match.match;
                }
            }
        }
        return null;
    }

    public UnmatchedIndexMap getBestMutualMatch()
    {
        return this.bestMutualMatch;
    }

    private static class Match implements Comparable<Match>
    {
        private static final SerializableComparator<Match> MATCH_COMPARATOR = Comparators.chain(
                Comparators.reverse(Comparators.byIntFunction(match -> match.matchScore)),
                Comparators.byIntFunction(match -> Math.max(match.match.getRhsIndex(), match.match.getLhsIndex())));

        private final int matchScore;
        private final UnmatchedIndexMap match;

        private Match(int matchScore, UnmatchedIndexMap match)
        {
            this.matchScore = matchScore;
            this.match = match;
        }

        @Override
        public int compareTo(Match that)
        {
            return MATCH_COMPARATOR.compare(this, that);
        }
    }
}
