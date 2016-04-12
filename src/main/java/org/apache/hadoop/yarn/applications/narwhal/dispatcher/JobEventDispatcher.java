package org.apache.hadoop.yarn.applications.narwhal.dispatcher;

import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.event.EventHandler;


public class JobEventDispatcher implements EventHandler<JobEvent>{

  private NAppMaster.AppContext context;

  public JobEventDispatcher(NAppMaster.AppContext context) {
    this.context = context;
  }

  @Override
  public void handle(JobEvent event) {
    ((EventHandler<JobEvent>)context.getJob()).handle(event);
  }
}
