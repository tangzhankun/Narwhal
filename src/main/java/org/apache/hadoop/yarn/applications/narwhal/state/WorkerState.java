package org.apache.hadoop.yarn.applications.narwhal.state;


public enum WorkerState {
  NEW,
  SCHEDULED,
  RUNNING,
  SUCCEED,
  FAILED,
  KILLED,
  ERROR
}
