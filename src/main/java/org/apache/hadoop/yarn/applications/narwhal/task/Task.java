package org.apache.hadoop.yarn.applications.narwhal.task;


import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;
import org.apache.hadoop.yarn.applications.narwhal.worker.Worker;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;

public interface Task {
  TaskId getID();
  TaskState getStatus();
  Worker getWorker(WorkerId workerId);
  Container getContainer();
}
