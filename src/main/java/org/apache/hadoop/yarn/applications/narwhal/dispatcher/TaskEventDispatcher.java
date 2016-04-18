package org.apache.hadoop.yarn.applications.narwhal.dispatcher;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.task.Task;
import org.apache.hadoop.yarn.event.EventHandler;


public class TaskEventDispatcher implements EventHandler<TaskEvent>{

  private static final Log LOG = LogFactory.getLog(TaskEventDispatcher.class);

  private NAppMaster.AppContext context;

  public TaskEventDispatcher(NAppMaster.AppContext context) {
    this.context = context;
  }

  @Override
  public void handle(TaskEvent taskEvent) {
    LOG.info("dispatch TaskEvent: " + taskEvent);
    Task task = context.getJob().getTask(taskEvent.getTaskID());
    ((EventHandler<TaskEvent>)task).handle(taskEvent);
  }
}
