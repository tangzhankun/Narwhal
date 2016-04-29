package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

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
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

public class ActionResolve implements ClientAction {
	
	private static final Log LOG = LogFactory.getLog(ActionResolve.class);
	
	private Options opts;
	private RegistryOperations registryOperations;
	private Configuration conf;
	private String appName;
	
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
			throw new IllegalArgumentException("specified application name");
		}
		appName = cliParser.getOptionValue("appName");
		
		registryOperations = createRegistryOperationsInstance();
		registryOperations.start();
		
		return true;
	}

	@Override
	public boolean execute() throws YarnException, IOException {
		String currentUser = RegistryUtils.currentUser();
		String path = RegistryUtils.servicePath(currentUser, serviceType, appName);
		ServiceRecord record = registryOperations.resolve(path);
		registryOperations.stop();
		if(record == null) {
			return false;
		}
		LOG.info(record);
		return true;
	}

  protected RegistryOperations createRegistryOperationsInstance() {
    return RegistryOperationsFactory.createInstance("YarnRegistry", conf);
  }
	
}
