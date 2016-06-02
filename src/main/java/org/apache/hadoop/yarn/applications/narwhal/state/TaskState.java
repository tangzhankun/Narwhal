package org.apache.hadoop.yarn.applications.narwhal.state;


public enum TaskState {
  NEW(0.0f),
  SCHEDULED(0.25f),
  READY(0.5f),
  FAILED(1.0f),
  RUNNING(0.75f),
  SUCCEED(1.0f),
  KILLED(1.0f),
  ERROR(1.0f);

  private float progress = 0.0f;

  TaskState(float progress) {
    this.progress = progress;
  }

  public float getProgress() {
    return progress;
  }
}
