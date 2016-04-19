package org.apache.hadoop.yarn.applications.narwhal.worker;


import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;

public class WorkerId extends ExecutorID {

  private TaskId taskId;

  public WorkerId(TaskId taskId, int id) {
    super(id);
    this.taskId = taskId;
  }

  public TaskId getTaskId() {
    return taskId;
  }

  public String toString() {
    return taskId.toString() + "_worker_" + id;
  }
}
