package org.apache.hadoop.yarn.applications.narwhal.config;

/**
 * Created by zyluo on 4/18/16.
 */
public class BuilderException extends Exception {
    /**
     *  Constructs a BuilderException with no detail
     *  message.
     */
    public BuilderException() {
    }

    /**
     *  Constructs a BuilderException with the
     *  specified detail message.
     *  @param s detail message
     */
    BuilderException(String s) {
        super(s);
    }
}
