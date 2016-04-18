package org.apache.hadoop.yarn.applications.narwhal.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.narwhal.worker.Worker;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 *
 */
public class WorkerEvent extends AbstractEvent<WorkerEventType>{

  private WorkerId workerId;

  private Container container;

  public WorkerEvent(WorkerId workerId, WorkerEventType type) {
    super(type);
    this.workerId = workerId;
  }

  public WorkerId getWorkerId() {
    return workerId;
  }

  public void setContainer(Container container) {
    this.container = container;
  }

  public Container getContainer() {
    return container;
  }
}
