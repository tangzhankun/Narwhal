package org.apache.hadoop.yarn.applications.narwhal.event;


import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.AbstractEvent;

public class ContainerAllocatorEvent extends AbstractEvent<ContainerAllocatorEventType>{
  private ExecutorID id;

  //requestor may specify node filtering string
  private String hostname;

  private Resource capability;

  private int priority;

  public ContainerAllocatorEvent(ExecutorID id, ContainerAllocatorEventType type) {
    super(type);
    this.id = id;
  }

  public ExecutorID getId() {
    return id;
  }

  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  public String getHostname() {
    return hostname;
  }

  public void setCapability(Resource capability) {
    this.capability = capability;
  }

  public Resource getCapability() {
    return capability;
  }

  public void setPriority(int priority) {
    this.priority = priority;
  }

  public int getPriority() {
    return priority;
  }

  public String toString() {
    String str = super.toString();
    String token = "";
    if (id instanceof TaskId) {
      token = "task";
    } else {
      token = "worker";
    }

    str = str + ", " + token +"Id: " + token +"_" + id.getId();

    return str;
  }

}
