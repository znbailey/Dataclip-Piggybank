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

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;
import org.arabidopsis.ahocorasick.AhoCorasick;
import org.arabidopsis.ahocorasick.SearchResult;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.Set;

/**
 * Construct with a pipe-delimited list of strings to search for
 * preceeded by a value to emit if any of those strings are found, delimited by
 * semicolons
 *
 * dogs=[terrier|retriever|pit bull];cats=[tabby|mainecoon|tuxedo];birds=[parakeet|parrot|cuckoo]
 *
 * Pig usage example:
 *
 * register /path/to/dataclip-piggybank.jar;
 * define AC_MATCHER com.dataclip.piggybank.AHO_CORASICK('dogs=[terrier|retriever|pit bull];cats=[tabby|mainecoon|tuxedo];birds=[parakeet|parrot|cuckoo]');
 * strings = LOAD 'strings.txt' USING PigStorage() as (string:chararray);
 * tagged_strings = FOREACH strings GENERATE string, AC_MATCHER(string) as tags;
 *
 * For a strings.txt file with the following contents:
 *
 * terrier parakeet
 * hello
 * goodbye
 * tabby
 * pit bull
 *
 * The above pig script will produce:
 *
 * (terrier parakeet,{(dogs),(birds)})
 * (hello,{})
 * (goodbye,{})
 * (tabby,{(cats)})
 * (pit bull,{(dogs)})
 */
public class AHO_CORASICK extends EvalFunc<DataBag> {

    private static final Charset UTF8 = Charset.forName("utf-8");

    TupleFactory mTupleFactory = TupleFactory.getInstance();
    BagFactory mBagFactory = BagFactory.getInstance();

    private final AhoCorasick<String> searcher;

    public AHO_CORASICK(String tokens) {
        searcher = new AhoCorasick<String>();
        for ( String block : tokens.split(";") ) {
            //block is of the form "dogs=[terrier|retriever|pit bull]"
            String[] resultAndNeedles = block.split("=");
            String result = resultAndNeedles[0];
            String bracketedNeedles = resultAndNeedles[1];
            //remove leading and trailing brackets from needles
            String needles = bracketedNeedles.substring(1, bracketedNeedles.length()-1);
            for ( String needle : needles.split("\\|")) {
                searcher.add(needle.getBytes(UTF8), result);
            }
        }
        searcher.prepare();
    }

    @Override
    public DataBag exec(Tuple input) throws IOException {
        if ( input == null || input.size() == 0 || input.get(0) == null ) {
            return null;
        }

        DataBag bag = mBagFactory.newDefaultBag();
        try {
            String haystack = (String) input.get(0);
            Iterator<SearchResult<String>> results = searcher.search(haystack.getBytes(UTF8));
            while ( results.hasNext() ) {
                Set<String> outputs = results.next().getOutputs();
                for ( String output : outputs ) {
                    bag.add(mTupleFactory.newTuple(output));
                }
            }
        } catch (Throwable t ) {
            t.printStackTrace();
            throw WrappedIOException.wrap("Could not process tuple: " + input, t);
        }
        return bag;
    }
}
