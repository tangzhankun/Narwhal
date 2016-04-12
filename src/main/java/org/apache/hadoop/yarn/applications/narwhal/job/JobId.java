package org.apache.hadoop.yarn.applications.narwhal.job;


import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;

public class JobId {

  private ApplicationAttemptId applicationAttemptId;

  public JobId(ApplicationAttemptId appAttemptId) {
    this.applicationAttemptId = appAttemptId;
  }

  public ApplicationId getAppId() {
    return applicationAttemptId.getApplicationId();
  }

  public int getId() {
    return applicationAttemptId.getApplicationId().getId();
  }

  public String toString() {
    return getAppId().toString();
  }

}
