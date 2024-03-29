package org.apache.hadoop.yarn.applications.narwhal;

import java.io.IOException;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.client.ActionListRegistedApp;
import org.apache.hadoop.yarn.applications.narwhal.client.ActionResolve;
import org.apache.hadoop.yarn.applications.narwhal.client.ActionSubmitApp;
import org.apache.hadoop.yarn.applications.narwhal.client.ClientAction;
import org.apache.hadoop.yarn.applications.narwhal.common.Log4jUtil;
import org.apache.hadoop.yarn.exceptions.YarnException;

/**
 * This is the Narwhal Client
 */
public class NClient {

  private static final Log LOG = LogFactory.getLog(NClient.class);

  private ClientAction action;

  public boolean init(String[] args) throws ParseException {

    String mainCmd = args[0];
    switch (mainCmd) {
      case ClientAction.RUN:
        action = new ActionSubmitApp();
        break;
      case ClientAction.RESOLVE:
        action = new ActionResolve();
        break;
      case ClientAction.REGISTRY:
        action = new ActionListRegistedApp();
        break;
      default:
        throw new IllegalArgumentException("unknown command");
    }
    return action.init(args);
  }

  public boolean run() throws YarnException, IOException, InterruptedException {
    return action.execute();
  }

  public static void main(String[] args) {
    Log4jUtil.loadProperties(NClient.class, "/Nlog4j.properties");
    boolean result = false;
    try {
      NClient nClient = new NClient();
      boolean inited = nClient.init(args);
      if (inited) {
        result = nClient.run();
      }
    } catch (ParseException | YarnException | IOException | InterruptedException e) {
      e.printStackTrace();
    }
    if (result) {
      LOG.info("Command [" + args[0] + "]  executed successfully");
      System.exit(0);
    }
    LOG.error("Command [" + args[0] + "] failed to executed successfully");
    System.exit(2);
  }

}
