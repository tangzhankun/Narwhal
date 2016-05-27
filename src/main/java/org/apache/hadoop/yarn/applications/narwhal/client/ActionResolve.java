package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.common.NarwhalConstant;
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
    Map<String, ServiceRecord> containers = registryOperator.resolveContainers();
    if (containers == null) {
      LOG.info(applicationId + " cannot be found");
    } else {
      LOG.info(formatOutput(containers));
    }
    return true;
  }

  private String formatOutput(Map<String, ServiceRecord> containers) {
    String format = "  %-40s %-20s %-40s %-25s %-15s %-15s %s\n";
    StringBuilder builder = new StringBuilder().append("\n\n");
    builder.append(String.format(format, NarwhalConstant.CONTAINER_ID, NarwhalConstant.IMAGE, NarwhalConstant.COMMAND, NarwhalConstant.CREATED, NarwhalConstant.STATUS, NarwhalConstant.HOST, NarwhalConstant.PORT));
    Set<String> containerIds = containers.keySet();
    for (String containerId : containerIds) {
      ServiceRecord record = containers.get(containerId);
      String createdTime = record.get(NarwhalConstant.CREATED);
      String host = record.get(NarwhalConstant.HOST);
      String port = record.get(NarwhalConstant.PORT);
      String image = record.get(NarwhalConstant.IMAGE);
      String status = record.get(NarwhalConstant.STATUS);
      String command = record.get(NarwhalConstant.COMMAND);
      builder.append(String.format(format, containerId, image, command, createdTime, status, host, port));
    }
    return builder.toString();
  }

}
