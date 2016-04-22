package org.apache.hadoop.yarn.applications.narwhal.event;


import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.event.AbstractEvent;


public class JobEvent extends AbstractEvent<JobEventType>{
  private JobId jobId;

  private NarwhalConfig narwhalConfig;
  public JobEvent(JobId jobId, JobEventType type) {
    super(type);
    this.jobId = jobId;
  }
  public JobId getJobId() {
    return jobId;
  }

  public NarwhalConfig getNarwhalConfig() {
    return narwhalConfig;
  }

  public void setNarwhalConfig(NarwhalConfig narwhalConfig) {
    this.narwhalConfig = narwhalConfig;
  }
}
