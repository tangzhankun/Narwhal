package org.apache.hadoop.yarn.applications.narwhal.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

/**
 *
 */
public class NTaskImpl implements Task, EventHandler<TaskEvent>{

  private static final Log LOG = LogFactory.getLog(NTaskImpl.class);

  private TaskId taskId;
  private String hostname;
  private String rackname;
  private String port;

  private final EventHandler eventHandler;

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;

  private static final ErrorTransition ERROR_TRANSITION =
      new ErrorTransition();

  public NTaskImpl(JobId jobId, int id, EventHandler eventHandler) {
    this.eventHandler = eventHandler;
    this.taskId = new TaskId(jobId, id);
    this.stateMachine = stateMachineFactory.make(this);
  }

  private static class ErrorTransition implements
      SingleArcTransition<NTaskImpl, TaskEvent> {

    @Override
    public void transition(NTaskImpl nTask, TaskEvent taskEvent) {

    }
  }

  protected static final StateMachineFactory<NTaskImpl, TaskState, TaskEventType, TaskEvent>
      stateMachineFactory
      = new StateMachineFactory<NTaskImpl, TaskState, TaskEventType, TaskEvent>(TaskState.NEW);
      //Transitions from NEW


  @Override
  public void handle(TaskEvent taskEvent) {

  }

  @Override
  public TaskId getID() {
    return null;
  }

  @Override
  public TaskState getStatus() {
    return null;
  }
}
