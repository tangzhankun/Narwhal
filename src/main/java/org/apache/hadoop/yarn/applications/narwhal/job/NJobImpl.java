package org.apache.hadoop.yarn.applications.narwhal.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.state.JobState;
import org.apache.hadoop.yarn.applications.narwhal.task.Task;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.StateMachine;

import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;


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
  private LinkedHashMap<TaskId, Task> tasks = new LinkedHashMap<TaskId, Task>();

  public NJobImpl(String name, ApplicationAttemptId appAttemptId,
                  Configuration conf, EventHandler eventHandler) {
    this.name = name;
    this.jobId = new JobId(appAttemptId);
    this.conf = conf;
    this.eventHandler = eventHandler;
    this.stateMachine = null;
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

  protected void addTask(Task task) {
    tasks.put(task.getID(), task);
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  public JobState getInternalState() {
    return getStateMachine().getCurrentState();
  }

  @Override
  public void handle(JobEvent jobEvent) {
    LOG.info("Processing " + jobEvent.getJobId() + "of type" + jobEvent.getType());

    try {
      writeLock.lock();
      JobState oldState = getInternalState();
      getStateMachine().doTransition(jobEvent.getType(), jobEvent);
      if (oldState != getInternalState()) {
        LOG.info(jobId + "Job Transitioned from " + oldState + " to "
            + getInternalState());
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
