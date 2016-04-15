package org.apache.hadoop.yarn.applications.narwhal;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.Job;
import org.apache.hadoop.yarn.applications.narwhal.job.NJobImpl;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerAllocator;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerLauncher;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.log4j.LogManager;

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
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private Job job;
  protected ApplicationAttemptId applicationAttemptId;


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

  protected void createContainerAllocator() {
    this.containerAllocator = new ContainerAllocator(context);
  }

  protected void startContainerAllocator() throws Exception {
    containerAllocator.start();
  }

  protected void createContainerLauncher() {
    this.containerLauncher = new ContainerLauncher(context);
  }

  protected void startContainerLauncher() throws Exception {
    containerLauncher.start();
  }

  protected void createJob() {

    job = new NJobImpl("Narwhal job", null, conf, context.getEventHandler());
    //init job event
    jobEventDispatcher.handle(new JobEvent(job.getID(), JobEventType.JOB_INIT));
  }

  protected void kickOffTheBall() {
    JobEvent startJobEvent = new JobEvent(job.getID(), JobEventType.JOB_START);
    dispatcher.getEventHandler().handle(startJobEvent);
  }

  private boolean parseOptions(String[] args) {
    Map<String, String> envs = System.getenv();
    ContainerId containerId = ContainerId.fromString(
        envs.get(ApplicationConstants.Environment.CONTAINER_ID.name()));
    applicationAttemptId = containerId.getApplicationAttemptId();
    return true;
  }

  public void init(String[] args) {
    parseOptions(args);
    createDispatcher();
    startDispatcher();
    createJobEventDispatcher();
    createTaskEventDispatcher();
    registerJobEventDispatcher();
    registerTaskEventDispatcher();
    createContainerAllocator();
    createContainerLauncher();
    LOG.info("Narwhal AM inited");
  }

  public void start() throws Exception {
    startContainerAllocator();
    startContainerLauncher();
    createJob();
    LOG.info("Narwhal AM started");
  }

  public void stop() {
    ((AbstractService)dispatcher).stop();
  }

  public boolean finish() {
    stop();
    return true;
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
