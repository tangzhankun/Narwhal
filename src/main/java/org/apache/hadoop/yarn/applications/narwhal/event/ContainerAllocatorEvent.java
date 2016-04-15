package org.apache.hadoop.yarn.applications.narwhal.event;


import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ContainerAllocatorEvent extends AbstractEvent<ContainerAllocatorEventType>{
  private TaskId taskId;

  public ContainerAllocatorEvent(TaskId taskId, ContainerAllocatorEventType type) {
    super(type);
    this.taskId = taskId;
  }

  public TaskId getId() {
    return taskId;
  }

  public String toString() {
    return super.toString() + ", taskId: " + taskId.toString();
  }

}
