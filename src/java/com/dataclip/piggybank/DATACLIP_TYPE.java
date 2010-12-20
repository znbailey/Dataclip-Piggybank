package com.dataclip.piggybank;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 *
 */
public class DATACLIP_TYPE extends EvalFunc<String> {

    @Override
    public String exec(Tuple tuple) throws IOException {
        if ( tuple == null || tuple.size() == 0 || tuple.get(0) == null ) {
            return null;
        }

        String data = (String) tuple.get(0);
        if ( data.startsWith("HTTP/1") ) {
            return "http:headers";
        } else if ( data.startsWith("<script") ) {
            return "html:script";
        } else if ( data.startsWith("<!") ) {
            return "html:comment";
        } else if ( data.startsWith("<form") ) {
            return "html:form";
        } else if ( data.startsWith("<iframe") ) {
            return "html:iframe";
        } else if ( data.startsWith("<object") ) {
            return "html:object";
        } else {
            return "unknown";
        }
        
    }
}
