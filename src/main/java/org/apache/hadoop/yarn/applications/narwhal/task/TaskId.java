package org.apache.hadoop.yarn.applications.narwhal.task;


import org.apache.hadoop.yarn.applications.narwhal.job.JobId;

public class TaskId extends ExecutorID {

  private JobId jobId;

  public JobId getJobId() {
    return jobId;
  }

  public TaskId(JobId jobId, int id) {
    super(id);
    this.jobId = jobId;
  }

  public String toString() {
    return this.jobId + "_task_" + this.id;
  }

}
