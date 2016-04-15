package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ContainerAllocator extends EventLoop implements EventHandler<ContainerAllocatorEvent>{

  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private AMRMClientAsync amRMClientAsync;

  private List<TaskId> pendingTasks = Collections.synchronizedList(new LinkedList<TaskId>());

  @Override
  public void handle(ContainerAllocatorEvent containerAllocatorEvent) {
    try {
      eventQueue.put(containerAllocatorEvent);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected AMRMClientAsync createAMRMClientAsync() {
    AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
    amRMClientAsync = AMRMClientAsync.createAMRMClientAsync(1000,allocListener);
    amRMClientAsync.init(context.getConf());
    return amRMClientAsync;
  }

  public ContainerAllocator(NAppMaster.AppContext context) {
    super(context);
  }

  @Override
  public void processEvent(AbstractEvent event) {
    ContainerAllocatorEvent CAEvent = (ContainerAllocatorEvent)event;
    LOG.info("Processing the event " + CAEvent);
    switch (CAEvent.getType()) {
      case CONTAINERALLOCATOR_REQEUST:
        setupAndAddContainer(CAEvent);
        break;
      case CONTAINERALLOCATOR_DEALLOCATE:
        releaseContainer();
        break;
    }
  }

  @Override
  public void startClientAsync() {
    createAMRMClientAsync().start();
  }

  private void setupAndAddContainer(ContainerAllocatorEvent event) {
    LOG.info("allocate container from AM");
    //amRMclient.addContainerRequest()
    pendingTasks.add(event.getId());
  }

  private void releaseContainer() {
    LOG.info("release container");
    //amRMClient.releaseAssignedContainer or removeContainerRequest?
  }

  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private final Log LOG = LogFactory.getLog(RMCallbackHandler.class);

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {

    }

    @Override
    public void onContainersAllocated(List<Container> list) {
      //assign containers to tasks
      for (Container allocatedContainer : list) {
        TaskId taskId = pendingTasks.get(0);
        taskId.setContainerId(allocatedContainer.getId());
        TaskEvent taskEvent = new TaskEvent(taskId, TaskEventType.TASK_SETUP);
        taskEvent.setContainer(allocatedContainer);
        eventHandler.handle(taskEvent);
      }
    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void onError(Throwable throwable) {

    }
  }

}
