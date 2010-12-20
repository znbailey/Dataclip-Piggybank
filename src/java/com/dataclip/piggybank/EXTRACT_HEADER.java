package com.dataclip.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 *
 */
public class EXTRACT_HEADER extends EvalFunc<String> {

    private static final Pattern HTTP_HEADER_PATTERN = Pattern.compile(" ([\\w\\-]+): ");

    private final String headerName;

    public EXTRACT_HEADER(String headerName) {
        this.headerName = headerName;
    }

    @Override
    public String exec(Tuple tuple) throws IOException {
        if ( tuple == null || tuple.size() < 1 || tuple.get(0) == null ) {
            return null;
        }

        String allHeaders = (String) tuple.get(0);
        String[] beforeAndAfter = allHeaders.split(" " + headerName + ": ");
        if ( beforeAndAfter.length == 1 ) {
            return null;
        } else {
            return HTTP_HEADER_PATTERN.split(beforeAndAfter[1])[0];
        }
    }

}
