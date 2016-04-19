package org.apache.hadoop.yarn.applications.narwhal.job;


import org.apache.hadoop.yarn.applications.narwhal.task.Task;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;

public interface Job {
  String getName();
  JobId getID();
  Task getTask(TaskId taskId);
}
