package org.apache.hadoop.yarn.applications.narwhal.state;


public enum JobState {
  NEW,
  INITED,
  KILLED,
  STARTED,
  FAILED,
  SUCCEED,
  ERROR
}
