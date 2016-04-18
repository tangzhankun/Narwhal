package org.apache.hadoop.yarn.applications.narwhal.worker;


import org.apache.hadoop.yarn.applications.narwhal.state.WorkerState;

public interface Worker {
  WorkerId getID();
  WorkerState getStatus();
}
