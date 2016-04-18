package org.apache.hadoop.yarn.applications.narwhal.task;


import org.apache.hadoop.yarn.api.records.ContainerId;

public class ExecutorID {
  protected int id;
  protected ContainerId containerId;

  public ExecutorID(int id) {
    this.id = id;
  }

  public int getId() {
    return id;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }
}
