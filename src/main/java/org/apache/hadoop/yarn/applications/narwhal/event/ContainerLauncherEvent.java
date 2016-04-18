package org.apache.hadoop.yarn.applications.narwhal.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;


public class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {

  private ExecutorID id;

  private Container container;

  public ContainerLauncherEvent(ExecutorID id, Container container,
                                ContainerLauncherEventType type) {
    super(type);
    this.id = id;
    this.container = container;
  }

  public ExecutorID getId() {
    return id;
  }

  public String toString() {
    return super.toString() + ", executorId: " + id;
  }

}
