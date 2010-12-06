/*
 * Copyright 2010 Dataclip, LLC http://dataclip.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dataclip.piggybank;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Performs an indexOf check on a given string with the supplied
 * needles. This is generally faster than attempting to construct
 * a regular expression when checking for a number of static strings.
 *
 * Usage:
 *
 * Pig usage example:
 *
 * register /path/to/dataclip-piggybank.jar;
 * define CONTAINS_MATCH com.dataclip.piggybank.CONTAINS_ANY('foo|bar');
 * haystack = LOAD 'strings.txt' USING PigStorage() as (string:chararray);
 * matches = FILTER haystack BY CONTAINS_MATCH(string);
 */
public class CONTAINS_ANY extends FilterFunc {

    private final String[] needles;

    public CONTAINS_ANY(String needles) {
        this.needles = needles.toLowerCase().split("\\|");
    }

    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        if ( tuple == null || tuple.size() == 0 || tuple.get(0) == null ) {
            return false;
        }

        String haystack = (String) tuple.get(0);
        String lowerHaystack = haystack.toLowerCase();
        for ( String needle : needles ) {
            if ( lowerHaystack.indexOf(needle) != -1 ) {
                return true;
            }
        }
        return false;
    }

}
