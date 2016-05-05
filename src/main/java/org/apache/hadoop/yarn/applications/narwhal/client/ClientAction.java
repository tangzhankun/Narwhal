package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.yarn.exceptions.YarnException;

public interface ClientAction {

  public static String RUN = "run";
  public static String RESOLVE = "resolve";

  public boolean init(String[] args) throws ParseException;

  public boolean execute() throws YarnException, IOException;

}
