package org.apache.hadoop.yarn.applications.narwhal.event;


import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 *
 */
public class TaskEvent extends AbstractEvent<TaskEventType> {

  private TaskId taskID;

  private Container container;

  public TaskEvent(TaskId taskID, TaskEventType type) {
    super(type);
    this.taskID = taskID;
  }

  public TaskId getTaskID() {
    return taskID;
  }

  public void setContainer(Container container) {
    this.container = container;
  }

  public Container getContainer() {
    return container;
  }

}
