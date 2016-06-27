package org.apache.hadoop.yarn.applications.narwhal.config;

import static org.junit.Assert.*;

/**
 * Created by zyluo on 6/3/16.
 */
public class TestNarwhalConfigParser {
    @org.junit.Test
    public void parse() throws Exception {
        NarwhalConfig config = NarwhalConfigParser.parse(NarwhalConfigCorpus.readmeInput);
        String actual = config.toString();
        String expected = NarwhalConfigCorpus.readmeOutput;
        assertEquals(expected, actual);
    }

    @org.junit.Test
    public void testVolumeMountConfigParse() throws Exception {
        NarwhalConfig config = NarwhalConfigParser.parse(NarwhalConfigCorpus.testVolumeMountInput);
        String actual = config.toString();
        String expected = NarwhalConfigCorpus.testVolumeMountOutput;
        assertEquals(expected, actual);
    }
}