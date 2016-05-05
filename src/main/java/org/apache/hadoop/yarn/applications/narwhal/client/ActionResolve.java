package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.applications.narwhal.registry.NRegistryOperator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ActionResolve implements ClientAction {

  private static final Log LOG = LogFactory.getLog(ActionResolve.class);

  private Options opts;
  private Configuration conf;
  private String appName;
  private NRegistryOperator registryOperator;

  private String serviceType = "narwhal-docker";

  public ActionResolve() {
    conf = new YarnConfiguration();

    opts = new Options();
    opts.addOption("appName", true, "query the service record");
  }

  @Override
  public boolean init(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (!cliParser.hasOption("appName")) {
      throw new IllegalArgumentException("no application name specified");
    }
    appName = cliParser.getOptionValue("appName");

    String currentUser = RegistryUtils.currentUser();
    registryOperator = new NRegistryOperator(currentUser, serviceType, appName, conf);
    return true;
  }

  @Override
  public boolean execute() throws YarnException, IOException {
    ServiceRecord record = registryOperator.resolve();
    if (record != null) {
      LOG.info(record);
    }
    return true;
  }

}
