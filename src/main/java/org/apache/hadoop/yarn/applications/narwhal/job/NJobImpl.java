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
    return null;
  }

  protected StateMachine<JobState, JobEventType, JobEvent> getStateMachine() {
    return stateMachine;
  }

  public JobState getInternalState() {
    return getStateMachine().getCurrentState();
  }

  @Override
  public void handle(JobEvent jobEvent) {
    JobState oldState = getInternalState();
    try {
      getStateMachine().doTransition(jobEvent.getType(), jobEvent);
    } catch (Exception e) {
      LOG.error("Can't handle this event at current state", e);
      eventHandler.handle(new JobEvent(this.jobId, JobEventType.JOB_ERROR));
    }
    if (oldState != getInternalState()) {
      LOG.info(jobId + "Job Transitioned from " + oldState + " to "
              + getInternalState());
    }
  }
}
