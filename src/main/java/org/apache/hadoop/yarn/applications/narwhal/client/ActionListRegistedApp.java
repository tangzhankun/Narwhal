package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.util.List;

import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ActionListRegistedApp implements ClientAction {

  private static final Log LOG = LogFactory.getLog(ActionListRegistedApp.class);

  private Configuration conf;
  private NRegistryOperator registryOperator;

  public ActionListRegistedApp() {
    conf = new YarnConfiguration();
  }

  @Override
  public boolean init(String[] args) throws ParseException {
    registryOperator = new NRegistryOperator(conf);
    return true;
  }

  @Override
  public boolean execute() throws YarnException, IOException, InterruptedException {
    LOG.info("registry list");
    List<String> narwhalApps = registryOperator.listNarwhalApps();
    if (narwhalApps.size() == 0) {
      LOG.info("no narwhal applications");
    } else {
      for (String appId : narwhalApps) {
        LOG.info(appId);
      }
    }
    return true;
  }

}
