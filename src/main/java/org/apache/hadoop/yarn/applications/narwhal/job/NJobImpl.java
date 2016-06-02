package org.apache.hadoop.yarn.applications.narwhal.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.applications.narwhal.common.DateUtil;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.common.NarwhalConstant;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.state.JobState;
import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;
import org.apache.hadoop.yarn.applications.narwhal.task.NTaskImpl;
import org.apache.hadoop.yarn.applications.narwhal.task.Task;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 *
 */
public class NJobImpl implements Job, EventHandler<JobEvent> {

  private static final Log LOG = LogFactory.getLog(NJobImpl.class);

  private final StateMachine<JobState, JobEventType, JobEvent> stateMachine;
  private final JobId jobId;
  private Configuration conf;
  private final EventHandler eventHandler;
  private final String name;
  private Lock readLock;
  private Lock writeLock;
  private LinkedHashMap<TaskId, Task> tasks = new LinkedHashMap<>();

  private int finishedTasksCount = 0;
  private NRegistryOperator nRegistryOperator;

  private static final ErrorTransition ERROR_TRANSITION =
      new ErrorTransition();

  private static final KillTransition KILL_TRANSITION =
      new KillTransition();

  private static class InitTransition implements
      MultipleArcTransition<NJobImpl, JobEvent, JobState> {

    @Override
    public JobState transition(NJobImpl nJob, JobEvent jobEvent) {
      LOG.info("**JobInitTransition**");
      setAppRecord(nJob);
      createTasks(nJob, jobEvent.getNarwhalConfig());
      return JobState.INITED;
    }
    
    private void setAppRecord(NJobImpl nJob){
      nJob.nRegistryOperator.setAppRecord(NarwhalConstant.CREATED, DateUtil.getCurrentTime());
      nJob.nRegistryOperator.updateApp();    	
    }

    private void createTasks(NJobImpl nJob, NarwhalConfig narwhalConfig) {
      int taskNum = narwhalConfig.getInstances();
      for (int i = 0; i < taskNum; i++) {
        Task task = new NTaskImpl(nJob.getID(), i, nJob.eventHandler,
            narwhalConfig.getCmd(), (int)narwhalConfig.getCpus(),
            (int)narwhalConfig.getMem(), 0, narwhalConfig.getImage(),
            narwhalConfig.isLocalImage(), narwhalConfig.getName(), nJob.nRegistryOperator);
        nJob.addTask(task);
      }
      LOG.info("create " + taskNum + " tasks.");
    }
  }

  private static class ErrorTransition implements
      SingleArcTransition<NJobImpl, JobEvent> {

    @Override
    public void transition(NJobImpl nJob, JobEvent jobEvent) {
      //error happens, stop the application
      LOG.info("JobErrorTransition");
    }
  }

  private static class KillTransition implements
      SingleArcTransition<NJobImpl, JobEvent> {

    @Override
    public void transition(NJobImpl nJob, JobEvent jobEvent) {
      //application was killed
    }
  }

  private static class StartTransition implements
      SingleArcTransition<NJobImpl, JobEvent> {

    @Override
    public void transition(NJobImpl nJob, JobEvent jobEvent) {
      //should send event to start each task
      LOG.info("**JobStartTransition**");
      for (Task task: nJob.tasks.values()){
        nJob.eventHandler.handle(new TaskEvent(task.getID(), TaskEventType.TASK_SCHEDULE));
      }
    }
  }

  private static class CompleteTransition implements
      MultipleArcTransition<NJobImpl, JobEvent, JobState> {

    @Override
    public JobState transition(NJobImpl nJob, JobEvent jobEvent) {
      LOG.info("**JobCompleteTransition**");
      //count the success finished task or handle failed task
      //if all tasks success, return SUCCEED
      nJob.finishedTasksCount++;
      List<Task> allTasks = nJob.getTasks();
      LOG.info("(" + nJob.finishedTasksCount + "/" + allTasks.size() + ") tasks finished");
      if (nJob.finishedTasksCount == allTasks.size()) {
        int count = 0;
        for (Task task : allTasks) {
          if (task.getStatus().equals(TaskState.SUCCEED)) {
            count++;
          }
        }
        if (count == allTasks.size()) {
          return JobState.SUCCEED;
        } else {
          return JobState.FAILED;
        }
      } else {
        return JobState.STARTED;
      }
    }
  }

  protected static final StateMachineFactory<NJobImpl, JobState, JobEventType, JobEvent>
      stateMachineFactory
      = new StateMachineFactory<NJobImpl, JobState, JobEventType, JobEvent>(JobState.NEW)
      //Transitions from NEW
      //NEW -> (INITED,FAILED): InitTransition
      .addTransition(JobState.NEW,
          EnumSet.of(JobState.INITED, JobState.FAILED),
          JobEventType.JOB_INIT,
          new InitTransition())
      //NEW -> ERROR: ErrorTransition
      .addTransition(JobState.NEW,
          JobState.ERROR,
          JobEventType.JOB_ERROR,
          ERROR_TRANSITION)
      //NEW -> KILLED: KillTransition
      .addTransition(JobState.NEW,
          JobState.KILLED,
          JobEventType.JOB_KILL,
          KILL_TRANSITION)

      //Transitions from INITED
      //INITED -> STARTED: StartTransition
      .addTransition(JobState.INITED,
          JobState.STARTED,
          JobEventType.JOB_START,
          new StartTransition())
      //INITED -> ERROR: ErrorTranstion
      .addTransition(JobState.INITED,
          JobState.ERROR,
          JobEventType.JOB_ERROR,
          ERROR_TRANSITION)

      //Transitions from STARTED
      //STARTED -> (STARTED,SUCCEED,FAILED): CompleteTransition
      .addTransition(JobState.STARTED,
          EnumSet.of(JobState.STARTED,JobState.SUCCEED, JobState.FAILED),
          JobEventType.JOB_COMPLETED,
          new CompleteTransition())
      //STARTED -> ERROR: ErrorTransition
      .addTransition(JobState.STARTED,
          JobState.ERROR,
          JobEventType.JOB_ERROR,
          ERROR_TRANSITION)
      //STARTED -> KILLED: KillTransition
      .addTransition(JobState.STARTED,
          JobState.KILLED,
          JobEventType.JOB_KILL,
          KILL_TRANSITION)
      .installTopology();

  public NJobImpl(String name, ApplicationAttemptId appAttemptId,
                  Configuration conf, EventHandler eventHandler) {
    this.name = name;
    this.jobId = new JobId(ConverterUtils.toApplicationAttemptId(appAttemptId.toString()));
    this.conf = conf;
    this.eventHandler = eventHandler;
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.stateMachine = stateMachineFactory.make(this);
    
    ApplicationId applicationId = appAttemptId.getApplicationId();
    nRegistryOperator = new NRegistryOperator(applicationId.toString(), conf);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public JobId getID() {
    return jobId;
  }

  @Override
  public Task getTask(TaskId taskId) {
    readLock.lock();
    try {
      return tasks.get(taskId);
    } finally {
      readLock.unlock();
    }
  }

  public float getProgress() {
    List<Task> tasks = getTasks();
    float progress = 0.0f;
    if (tasks.size() == 0) {
      return progress;
    }
    for (Task task : tasks) {
      TaskState state = task.getStatus();
      progress += state.getProgress();
    }
    return progress/tasks.size();
  }

  public List<Task> getTasks() {
    return new ArrayList<Task>(tasks.values());
  }

  protected void addTask(Task task) {
    tasks.put(task.getID(), task);
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  public JobState getStatus() {
    return getStateMachine().getCurrentState();
  }

  public int getFinishedTasksCount() {
    return finishedTasksCount;
  }
  @Override
  public void handle(JobEvent jobEvent) {
    LOG.info("Processing " + jobEvent.getJobId() + " of type " + jobEvent.getType());

    try {
      writeLock.lock();
      JobState oldState = getStatus();
      getStateMachine().doTransition(jobEvent.getType(), jobEvent);
      if (oldState != getStatus()) {
        LOG.info(jobId + " Job Transitioned from " + oldState + " to "
            + getStatus());
      }
    } catch (Exception e) {
      LOG.error("Can't handle this event at current state", e);
      eventHandler.handle(new JobEvent(this.jobId, JobEventType.JOB_ERROR));
    }
    finally {
      writeLock.unlock();
    }

  }
}
