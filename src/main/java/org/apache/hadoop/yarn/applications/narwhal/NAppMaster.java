package org.apache.hadoop.yarn.applications.narwhal;


import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.WorkerEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.job.Job;
import org.apache.hadoop.yarn.applications.narwhal.job.NJobImpl;
import org.apache.hadoop.yarn.applications.narwhal.registry.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerAllocator;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerLauncher;
import org.apache.hadoop.yarn.applications.narwhal.state.JobState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.log4j.LogManager;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * The Narwhal Application Master
 */
public class NAppMaster {

  private static final Log LOG = LogFactory.getLog(NAppMaster.class);
  
  private Configuration conf;
  private AppContext context;
  private Dispatcher dispatcher;
  private JobEventDispatcher jobEventDispatcher;
  private TaskEventDispatcher taskEventDispatcher;
  private WorkerEventDispatcher workerEventDispatcher;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private Job job;
  protected ApplicationAttemptId applicationAttemptId;
  protected NarwhalConfig narwhalConfig;
  private static final String configFilePath = "artifact.json";
  
  private NRegistryOperator registryOperator;

  public class AppContext{
    private final Configuration conf;
    public AppContext(Configuration conf) {
      this.conf = conf;
    }

    public EventHandler getEventHandler() {
      return dispatcher.getEventHandler();
    }

    public Job getJob(){
      return job;
    }

    public Configuration getConf() {
      return conf;
    }
  }

  public NAppMaster() {
    conf = new YarnConfiguration();
    context = new AppContext(conf);
  }

  protected void createDispatcher() {
    dispatcher = new AsyncDispatcher();
  }

  protected void startDispatcher() {
    ((AbstractService)dispatcher).init(conf);
    ((AbstractService)dispatcher).start();
  }

  protected void createJobEventDispatcher() {
    this.jobEventDispatcher = new JobEventDispatcher(context);
  }

  protected void registerJobEventDispatcher() {
    dispatcher.register(JobEventType.class, jobEventDispatcher);
  }

  protected void createTaskEventDispatcher() {
    this.taskEventDispatcher = new TaskEventDispatcher(context);
  }

  protected void registerTaskEventDispatcher() {
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
  }

  protected void createWorkerEventDispatcher() {
    this.workerEventDispatcher = new WorkerEventDispatcher(context);
  }

  protected void registerWorkerEventDispatcher() {
    dispatcher.register(WorkerEventType.class, workerEventDispatcher);
  }

  protected void createContainerAllocator() {
    this.containerAllocator = new ContainerAllocator(context);
  }

  protected void registerContainerAllocator() {
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
  }

  protected void startContainerAllocator() throws Exception {
    containerAllocator.start();
  }

  protected void createContainerLauncher() {
    this.containerLauncher = new ContainerLauncher(context);
  }

  protected void registerContainerLauncher() {
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);
  }

  protected void startContainerLauncher() throws Exception {
    containerLauncher.start();
  }

  protected void createJob() {
    job = new NJobImpl("Narwhal job", applicationAttemptId.toString(), conf, context.getEventHandler());
    //init job event
    JobEvent jobEvent = new JobEvent(job.getID(), JobEventType.JOB_INIT);
    jobEvent.setNarwhalConfig(narwhalConfig);
    jobEventDispatcher.handle(jobEvent);
  }

  protected void kickOffTheBall() {
    JobEvent startJobEvent = new JobEvent(job.getID(), JobEventType.JOB_START);
    startJobEvent.setNarwhalConfig(narwhalConfig);
    dispatcher.getEventHandler().handle(startJobEvent);
  }

  private boolean parseOptions(String[] args) throws ParseException, IOException {
    Map<String, String> envs = System.getenv();
    
    ContainerId containerId = ContainerId.fromString(
        envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    applicationAttemptId = containerId.getApplicationAttemptId();
    LOG.info("applicationAttemptId: " + applicationAttemptId);
    
    String configPath = "./" + configFilePath;
    narwhalConfig = deserializeObj(configPath);
    LOG.info("<----config file path : "+ configPath+ "---->");
    LOG.info("<----appname : "+ narwhalConfig.getName() + "---->");
    
    return true;
  }

  public void init(String[] args) throws ParseException, IOException {
    parseOptions(args);
    
    LOG.info("Before register service instance");
    registerServiceInstance(applicationAttemptId.getApplicationId());
    LOG.info("After register service instance");
    
    createDispatcher();
    startDispatcher();
    createJobEventDispatcher();
    createTaskEventDispatcher();
    createWorkerEventDispatcher();
    registerJobEventDispatcher();
    registerTaskEventDispatcher();
    registerWorkerEventDispatcher();
    createContainerAllocator();
    createContainerLauncher();
    registerContainerAllocator();
    registerContainerLauncher();
    LOG.info("Narwhal AM inited");
  }

  public void start() throws Exception {
    startContainerAllocator();
    startContainerLauncher();
    createJob();
    LOG.info("Narwhal AM started");
  }

  public void stop() {
    //nmClientAsync must first stop or
    //amRMClientAsync will report a "Interrupted while waiting for queue" error
    containerLauncher.stop();
    containerAllocator.stop();
    ((AbstractService)dispatcher).stop();
  }

  public boolean finish() throws InterruptedException {
    NJobImpl nJob = (NJobImpl)job;
    FinalApplicationStatus appStatus = FinalApplicationStatus.UNDEFINED;
    String msg = "";
    boolean shouldExit = false;
    boolean result = false;
    while (true) {
      JobState state = nJob.getStatus();
      if (state == JobState.SUCCEED) {
        appStatus = FinalApplicationStatus.SUCCEEDED;
        msg = "Narwhal succeeded with " + nJob.getTasks().size() + " tasks";
        result = true;
        shouldExit = true;
      } else if (state == JobState.ERROR |
          state == JobState.FAILED) {
        appStatus = FinalApplicationStatus.FAILED;
        msg = "Narwhal failed";
        result = false;
        shouldExit = true;
      }
      if (shouldExit) {
        containerAllocator.unregisterAM(appStatus, msg, "");
        LOG.info(msg);
        stop();
        return result;
      }
      Thread.sleep(5000);
    }
  }
  
  private NarwhalConfig deserializeObj(String path){
    NarwhalConfig narwhalConfig = null;
    try {
      ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
      narwhalConfig = (NarwhalConfig) ois.readObject();
      ois.close();
  	} catch (IOException | ClassNotFoundException e) {
  		e.printStackTrace();
  	}
    return narwhalConfig;
  }
  
  public void registerServiceInstance(ApplicationId appId) throws IOException {
 
    registryOperator = new NRegistryOperator(appId.toString(), conf);

    registryOperator.set("TEST", "TEST_VALUE");
    registryOperator.set("TEST1", "TEST_VALUE1");
    registryOperator.set("TEST2", "TEST_VALUE2");
    registryOperator.register(true);
    registryOperator.set("TEST", "TEST_VALUE_UPDATE");
    registryOperator.set("TEST3", "TEST_VALUE3");
    registryOperator.update();
  }
  
  public static void main(String[] args) {
    boolean result = false;
    try {
      NAppMaster nAM = new NAppMaster();
      nAM.init(args);
      nAM.start();
      nAM.kickOffTheBall();
      result = nAM.finish();
    } catch (Throwable t) {
      LOG.fatal("Error running Narwhal Application Master",t);
      LogManager.shutdown();
    }
    if (result) {
      LOG.info("Narwhal AM completed successfully. existing");
      System.exit(0);
    } else {
      LOG.info("Narwhal AM failed. existing");
      System.exit(2);
    }
  }
}
