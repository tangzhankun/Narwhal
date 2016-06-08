package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
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
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
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
import org.apache.hadoop.yarn.applications.narwhal.common.ImageUtil;
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

public class ActionSubmitApp implements ClientAction {

  private static final Log LOG = LogFactory.getLog(ActionSubmitApp.class);

  private Options opts;
  private Configuration conf;
  private String appMasterMainClass;
  private String appMasterJar;
  private String configFile;
  private NarwhalConfig narwhalConfig;
  private YarnClient yarnClient;

  private static final String appMasterJarPath = "NAppMaster.jar";
  private static final String configFilePath = "artifact.json";

  private int amPriority = 0;
  private String amQueue = "default";
  private int amMemory = 2048;
  private int amVCores = 2;

  public ActionSubmitApp() {
    this(new YarnConfiguration());
  }

  public ActionSubmitApp(Configuration conf) {
    this("org.apache.hadoop.yarn.applications.narwhal.NAppMaster", conf);
  }

  public ActionSubmitApp(String appMasterMainClass, Configuration conf) {
    this.conf = conf;
    this.appMasterMainClass = appMasterMainClass;

    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);

    opts = new Options();
    opts.addOption("jar", true, "Jar file containing the application master");
    opts.addOption("configFile", true, "specify predefined config file path");
  }

  @Override
  public boolean init(String[] args) throws ParseException {
    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException("No jar file specified for application master");
    }
    appMasterJar = cliParser.getOptionValue("jar");

    if (!cliParser.hasOption("configFile")) {
      throw new IllegalArgumentException("No config file specified");
    }
    configFile = cliParser.getOptionValue("configFile");
    String configFileContent = readConfigFileContent(configFile);
    narwhalConfig = parseConfigFile(configFileContent);

    if (narwhalConfig == null) {
      return false;
    }
    return true;
  }

  @Override
  public boolean execute() throws YarnException, IOException, InterruptedException {

    yarnClient.start();

    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    ApplicationId appId = appContext.getApplicationId();

    appContext.setApplicationName(narwhalConfig.getName());

    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
    FileSystem fs = FileSystem.get(conf);
    //upload the local docker image
    if (narwhalConfig.isLocalImage()) {
      boolean dockerImgUploaded = uploadDockerImage(fs, appId.toString(), narwhalConfig.getImage());
      if (dockerImgUploaded) {
        LOG.info("Local Docker image " + narwhalConfig.getImage() + " uploaded successfully");
      } else {
        LOG.info("Local Docker image " + narwhalConfig.getImage() + " upload failed, existing");
        System.exit(3);
      }
    }
    addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(), localResources, null);
    configFile = serializeObj(appId, narwhalConfig);
    addToLocalResources(fs, configFile, configFilePath, appId.toString(), localResources, null);

    Map<String, String> env = prepareEnv();
    List<String> commands = prepareCommands();

    ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
    appContext.setAMContainerSpec(amContainer);
    Resource capability = Resource.newInstance(amMemory, amVCores);
    appContext.setResource(capability);

    // set security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          LOG.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }

    Priority pri = Priority.newInstance(amPriority);
    appContext.setPriority(pri);
    appContext.setQueue(amQueue);

    yarnClient.submitApplication(appContext);

    return monitorApplicartion(appId);
  }

  private boolean uploadDockerImage(FileSystem fs, String appId, String image) throws IOException, InterruptedException {
    boolean uploaded = false;
    //run docker save
    LOG.info("uploading Docker image: " + image);
    //TODO: check the fsImageName to ensure it matches DFS restrictions. for instance, no ":" in it
    String fsImageName = ImageUtil.getFSFileName(image);
    String localTempTar = ImageUtil.getTempDir() + fsImageName;
    String saveCmd = "docker save -o " + localTempTar + " " + image;
    Process p = Runtime.getRuntime().exec(saveCmd);
    p.waitFor();
    int exitCode = p.exitValue();
    if (exitCode == 0) {
      //TODO: use md5 to check if we need upload
      String suffix = ImageUtil.getFSFilePathSuffix(narwhalConfig.getName(), appId, fsImageName);
      Path dst = new Path(fs.getHomeDirectory(), suffix);
      fs.copyFromLocalFile(new Path(localTempTar), dst);
      FileStatus scFileStatus = fs.getFileStatus(dst);
      //TODO: check the hdfs upload successfully
      uploaded = true;
    } else {
      BufferedReader reader = new BufferedReader(new InputStreamReader(
              p.getInputStream()));
      String s;
      while ((s = reader.readLine()) != null) {
        System.out.println("Docker save output: " + s);
      }
    }
    return uploaded;
  }

  private NarwhalConfig parseConfigFile(String configFileContent) {
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

  public String serializeObj(ApplicationId appId, NarwhalConfig config) {
    String confPath = "/tmp/" + appId.toString() + "_" + UUID.randomUUID().toString();
    try {
      ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(confPath));
      oos.writeObject(config);
      oos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return confPath;
  }

  public boolean monitorApplicartion(ApplicationId appId) throws YarnException, IOException {

    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.debug("Thread sleep in monitoring loop interrupted");
      }

      ApplicationReport report = yarnClient.getApplicationReport(appId);
      LOG.info("Got report:"
              + "appId=" + appId.getId()
              + ", appDiagnostics=" + report.getDiagnostics()
              + ", appQueue=" + report.getQueue()
              + ", progress= " + String.format("%.1f",report.getProgress()*100) + "%");
      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          LOG.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          LOG.info("Application did finished unsuccessfully." + " YarnState="
                  + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                  + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state
              || YarnApplicationState.FAILED == state) {
        LOG.info("Application did not finish." + " YarnState="
                + state.toString() + ", DSFinalStatus=" + dsStatus.toString()
                + ". Breaking monitoring loop");
        return false;
      }
    }
  }

  public List<String> prepareCommands() {
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    vargs.add("-Xmx128m");
    vargs.add(appMasterMainClass);
    vargs.add(">" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
//    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
    vargs.add("2>&1");
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
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
    for (String classPath : yarnClassPaths) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(classPath.trim());
    }
    env.put("CLASSPATH", classPathEnv.toString());
    return env;
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

}
