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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Crude, "Fuzzy" matching UDF for handling http/https URL/URIs
 * and extracting the host portion.
 */
public class URI_HOST extends EvalFunc<String> {

    private static final Pattern URI_PATTERN = Pattern.compile("http(s)?://(.+?)/(.*)", Pattern.CASE_INSENSITIVE);

    @Override
    public String exec(Tuple input) throws IOException {
        if ( input == null || input.size() == 0 || input.get(0) == null ) {
            return null;
        }

        String uriStr = (String) input.get(0);
        //danger, assumes lower-case
        if ( !(uriStr.startsWith("http://") || uriStr.startsWith("https://")) ) {
            uriStr = "http://" + uriStr;
        }
        try {
            URI uri = new URI(uriStr);
            return uri.getHost();
        } catch ( URISyntaxException uriSE ) {
            //"fuzzy" match
            Matcher m = URI_PATTERN.matcher(uriStr);
            if ( m.matches() ) {
                return m.group(2);
            }
            return null;
        } catch ( Throwable t ) {
            t.printStackTrace();
            throw WrappedIOException.wrap("Could not process tuple: " + input, t);
        }
    }
}
