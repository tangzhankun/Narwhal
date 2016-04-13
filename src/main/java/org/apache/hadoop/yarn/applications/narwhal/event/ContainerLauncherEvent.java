package org.apache.hadoop.yarn.applications.narwhal.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;


public class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {
  private TaskId taskID;
  private Container container;
  public ContainerLauncherEvent(TaskId taskId, Container container,
                                ContainerLauncherEventType type) {
    super(type);
    this.taskID = taskId;
    this.container = container;
  }

  public String toString() {
    return super.toString() + ", taskId: " + taskID;
  }

}
