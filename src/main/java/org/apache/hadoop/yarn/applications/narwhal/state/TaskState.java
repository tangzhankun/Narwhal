package org.apache.hadoop.yarn.applications.narwhal.state;


public enum TaskState {
  NEW,
  SCHEDULED,
  READY,
  FAILED,
  RUNNING,
  SUCCEED,
  KILLED,
  ERROR
}
