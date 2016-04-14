package org.apache.hadoop.yarn.applications.narwhal.task;


import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;

public class TaskId {

  private JobId jobId;

  private int id;

  private ContainerId containerId;

  public JobId getJobId() {
    return jobId;
  }

  public ContainerId getContainerId() {
    return containerId;
  }

  public void setContainerId(ContainerId containerId) {
    this.containerId = containerId;
  }

  public TaskId(JobId jobId, int id) {
    this.jobId = jobId;
    this.id = id;
  }


}
