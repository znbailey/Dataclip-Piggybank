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
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.WrappedIOException;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Collapses any consecutive string of linebreak characters and/or tabs into
 * a single space character.
 */
public class COLLAPSE extends EvalFunc<String> {

    private static final Pattern LINE_BREAK = Pattern.compile("[\r\n\t]+");

    @Override
    public String exec(Tuple input) throws IOException {
        if ( input == null || input.size() == 0 ) {
            return null;
        }

        try {
            String toCollapse = (String) input.get(0);
            if ( toCollapse == null ) {
                return null;
            }
            return LINE_BREAK.matcher(toCollapse).replaceAll(" ");
        } catch ( Throwable t ) {
            t.printStackTrace();
            throw WrappedIOException.wrap("Could not process tuple: " + input, t);
        }
    }

}
