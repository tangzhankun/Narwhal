package org.apache.hadoop.yarn.applications.narwhal.event;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;


public class ContainerLauncherEvent extends AbstractEvent<ContainerLauncherEventType> {

  private ExecutorID id;

  private Container container;

  private String userCmd;

  private String dockerImageName;

  private String resourceFileName;

  private String resourceFilePath;

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

  public Container getContainer() {
    return container;
  }

  public String getUserCmd() {
    return userCmd;
  }

  public void setUserCmd(String userCmd) {
    this.userCmd = userCmd;
  }

  public void setDockerImageName(String dockerImageName) {
    this.dockerImageName = dockerImageName;
  }

  public String getDockerImageName() {
    return dockerImageName;
  }

  public String getResourceFileName() {
    return resourceFileName;
  }

  public void setResourceFileName(String resourceFileName) {
    this.resourceFileName = resourceFileName;
  }

  public String getResourceFilePath() {
    return resourceFilePath;
  }

  public void setResourceFilePath(String resourceFilePath) {
    this.resourceFilePath = resourceFilePath;
  }
}
