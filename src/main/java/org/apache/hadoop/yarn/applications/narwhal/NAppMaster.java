package org.apache.hadoop.yarn.applications.narwhal;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.Job;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.log4j.LogManager;

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

  private Job job;

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

  }

  protected void startContainerAllocator() {

  }

  protected void createContainerLauncher() {

  }

  protected void startContainerLauncher() {

  }

  protected void createJob() {

  }

  protected void kickOffTheBall() {

  }

  private boolean parseOptions(String[] args) {
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

  public void start() {
    createJob();
    startContainerAllocator();
    startContainerLauncher();
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
