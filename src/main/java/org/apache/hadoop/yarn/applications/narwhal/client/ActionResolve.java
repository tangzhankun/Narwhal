package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.applications.narwhal.common.NarwhalConstant;
import org.apache.hadoop.yarn.applications.narwhal.registry.NRegistryOperator;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ActionResolve implements ClientAction {

  private static final Log LOG = LogFactory.getLog(ActionResolve.class);

  private Options opts;
  private Configuration conf;
  private String applicationId;
  private NRegistryOperator registryOperator;

  public ActionResolve() {
    conf = new YarnConfiguration();

    opts = new Options();
    opts.addOption("applicationId", true, "query the service record");
  }

  @Override
  public boolean init(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (!cliParser.hasOption("applicationId")) {
      throw new IllegalArgumentException("no application id specified");
    }
    applicationId = cliParser.getOptionValue("applicationId");

    registryOperator = new NRegistryOperator(applicationId, conf);
    return true;
  }

  @Override
  public boolean execute() throws YarnException, IOException {
    ServiceRecord record = registryOperator.resolveApp();
    List<String> listAllContainers = registryOperator.listAllContainers();
    List<String> listNarwhalApps = registryOperator.listNarwhalApps();
    Map<String, ServiceRecord> resolveContainers = registryOperator.resolveContainers();
    LOG.info("listAllContainers" + listAllContainers.toString());
    LOG.info("listNarwhalApps" + listNarwhalApps.toString());
    LOG.info("resolveContainers" + resolveContainers.values());
    LOG.info("resolveApp" + record);
    return true;
  }

}
