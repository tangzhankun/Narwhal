package org.apache.hadoop.yarn.applications.narwhal.task;


import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;

public interface Task {
  TaskId getID();
  TaskState getStatus();
}
