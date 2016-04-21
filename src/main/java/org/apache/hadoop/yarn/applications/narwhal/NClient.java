package org.apache.hadoop.yarn.applications.narwhal;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.applications.narwhal.config.BuilderException;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfigBuilder;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfigParser;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.codehaus.jettison.json.JSONException;

/**
 * This is the Narwhal Client for submitting the AM
 */
public class NClient {
	
	private static final Log LOG = LogFactory.getLog(NClient.class);

	private Configuration conf;
	private String appMasterMainClass;
	private String appMasterJar;
	private String configFile;
	private NarwhalConfig narwhalConfig;
	private YarnClient yarnClient;
	private Options opts;
//	private String configFilePath = "~/tmp/" + UUID.randomUUID().toString();
	
	private static final String appMasterJarPath = "NAppMaster.jar";
	private static final String configFilePath = "artifact.json";
	
	private int amPriority = 0;
	private String amQueue = "default";
	private int amMemory = 10; 
	private int amVCores = 1;

	public NClient() {
		this(new YarnConfiguration());
	}

	public NClient(Configuration conf) {
		this("org.apache.hadoop.yarn.applications.narwhal.NAppMaster", conf);
	}

	public NClient(String appMasterMainClass, Configuration conf) {
		this.conf = conf;
		this.appMasterMainClass = appMasterMainClass;

		yarnClient = YarnClient.createYarnClient();
		yarnClient.init(conf);

		opts = new Options();
		opts.addOption("jar", true, "Jar file containing the application master");
		opts.addOption("configFile", true, "specify predefined config file path");
		
	}

	public boolean run() throws YarnException, IOException {
		yarnClient.start();
		
		YarnClientApplication app = yarnClient.createApplication();
		GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
		
		ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
		ApplicationId appId = appContext.getApplicationId();
		
		appContext.setApplicationName(narwhalConfig.getName());
		
		Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
		FileSystem fs = FileSystem.get(conf);
		addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(), localResources, null);
		addToLocalResources(fs, configFile, configFilePath, appId.toString(), localResources, null);
		
		Map<String, String> env = prepareEnv();
		List<String> commands = prepareCommands();
		
		ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
		appContext.setAMContainerSpec(amContainer);
	    Resource capability = Resource.newInstance(amMemory, amVCores);
	    appContext.setResource(capability);
	    Priority pri = Priority.newInstance(amPriority);
	    appContext.setPriority(pri);
	    appContext.setQueue(amQueue);
		
		yarnClient.submitApplication(appContext);
		
		return monitorApplicartion(appId);
	}
	
	public boolean monitorApplicartion(ApplicationId appId) throws YarnException, IOException{
		
		while (true) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				LOG.debug("Thread sleep in monitoring loop interrupted");
			}
			
			ApplicationReport report = yarnClient.getApplicationReport(appId);
			YarnApplicationState state = report.getYarnApplicationState();
			FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
			if (YarnApplicationState.FINISHED == state) {
				if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
					LOG.info("Application has completed successfully. Breaking monitoring loop");
					return true;        
				} else {
					LOG.info("Application did finished unsuccessfully."
							+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
							+ ". Breaking monitoring loop");
					return false;
		        }			  
			} else if (YarnApplicationState.KILLED == state	|| YarnApplicationState.FAILED == state) {
				LOG.info("Application did not finish."
						+ " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
						+ ". Breaking monitoring loop");
				return false;
			}			
		}
	}
	
	public List<String> prepareCommands() {
		Vector<CharSequence> vargs = new Vector<CharSequence>(30);
		vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
		vargs.add(appMasterMainClass);
		vargs.add("--appname " + narwhalConfig.getName());
		vargs.add("--container_memory" + narwhalConfig.getMem());
		vargs.add("--container_vcores" + narwhalConfig.getCpus());
		vargs.add("--instances_num " + narwhalConfig.getInstances());
		vargs.add("--command " + narwhalConfig.getCmd());
		vargs.add("--image " + narwhalConfig.getImage());
		vargs.add("--local " + narwhalConfig.isLocal());
	    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
	    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
	    
	    StringBuilder command = new StringBuilder();
	    for(CharSequence str : vargs) {
	    	command.append(str).append(" ");
	    }
	    
	    List<String> commands = new ArrayList<String>();
	    commands.add(command.toString());
	    
	    return commands;
	}
	
	public Map<String, String> prepareEnv() {
		Map<String, String> env = new HashMap<String, String>();
		
		StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
		String[] yarnClassPaths = conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH);
		for(String classPath : yarnClassPaths) {
			classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
			classPathEnv.append(classPath.trim());
		}
		env.put("CLASSPATH", classPathEnv.toString());
		return env;
	}

	public boolean init(String[] args) throws ParseException, IOException {
		
		CommandLine cliParser = new GnuParser().parse(opts, args);
		
		if (args.length == 0) {
			throw new IllegalArgumentException("No args specified for client to initialize");
		}

		if (!cliParser.hasOption("configFile")) {
			throw new IllegalArgumentException("No config file specified");
		}
		
		configFile = cliParser.getOptionValue("configFile");
		String configFileContent = readConfigFileContent(configFilePath);
		narwhalConfig = parseConfigFile(configFileContent);
		
		if (!cliParser.hasOption("jar")) {
			throw new IllegalArgumentException("No jar file specified for application master");
		}

		appMasterJar = cliParser.getOptionValue("jar");

		return true;
	}
	
	private NarwhalConfig parseConfigFile(String configFileContent){
		NarwhalConfigBuilder builder = new NarwhalConfigBuilder();
		NarwhalConfig config = null;
		try {
			new NarwhalConfigParser(builder).parse(configFileContent);
			config = builder.build();
			
		} catch (BuilderException | JSONException e) {
			// TODO
		}
		return config;
	}
	
//	private void saveConfigFileToHdfs(String configFileContent){
//		NarwhalConfigBuilder builder = new NarwhalConfigBuilder();
//		FileOutputStream fileOutputStream = null;
//		ObjectOutputStream objectOutputStream = null;
//		try {
//			new NarwhalConfigParser(builder).parse(configFileContent);
//			NarwhalConfig config = builder.build();
//			
//			fileOutputStream = new FileOutputStream(configFilePath);
//			objectOutputStream = new ObjectOutputStream(fileOutputStream);
//			objectOutputStream.writeObject(config);
//			
////			addToLocalResources(fs, configFilePath, configFileContent, configFileContent, null, configFileContent);
//			
//		} catch (BuilderException | JSONException | IOException e) {
//			// TODO
//		} finally {
//			try {
//				objectOutputStream.close();
//				objectOutputStream.close();
//			} catch (IOException e) {
//				e.printStackTrace();
//			}
//		}
//	}

	private String readConfigFileContent(String path) {

		String line = null;
		StringBuilder body = new StringBuilder();

		BufferedReader bufferReader = null;
		try {
			bufferReader = new BufferedReader(new FileReader(path));
			while ((line = bufferReader.readLine()) != null) {
				body.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				bufferReader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return body.toString();
	}

	private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId, Map<String, LocalResource> localResources, String resources) throws IOException {
		String suffix = narwhalConfig.getName() + "/" + appId + "/" + fileDstPath;
		Path dst = new Path(fs.getHomeDirectory(), suffix);
		if (fileSrcPath == null) {
			FSDataOutputStream ostream = null;
			try {
				ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
				ostream.writeUTF(resources);
			} finally {
				IOUtils.closeQuietly(ostream);
			}
		} else {
			fs.copyFromLocalFile(new Path(fileSrcPath), dst);
		}
		FileStatus scFileStatus = fs.getFileStatus(dst);
		LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
		localResources.put(fileDstPath, scRsrc);
	}

	public static void main(String[] args) {
		boolean result = false;
		try {
			NClient nClient = new NClient();
			boolean inited = nClient.init(args);
			if (inited) {
				result = nClient.run();
			}
		} catch (ParseException | IOException | YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    if (result) {
	    	LOG.info("Application completed successfully");
	        System.exit(0);
	    } 
	    LOG.error("Application failed to complete successfully");
	    System.exit(2);
	}
}
