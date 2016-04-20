package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public class ContainerAllocator extends EventLoop implements EventHandler<ContainerAllocatorEvent>{

  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private AMRMClientAsync amRMClientAsync;

  private AMRMClientAsync.CallbackHandler allocListener;

  private List<ExecutorID> pendingTasks = Collections.synchronizedList(new LinkedList<ExecutorID>());

  public static List<Container> containers = new ArrayList<>(1);

  static {
    //fake code
    for (int i = 1; i < 2; i++) {
      ApplicationId applicationId = ApplicationId.newInstance(i, i);
      ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, i);
      ContainerId containerId = ContainerId.newContainerId(applicationAttemptId, i);
      Container container = Container.newInstance(containerId, NodeId.newInstance("host", 5000),
          "host:80", Resource.newInstance(1024, 1), Priority.newInstance(0), null);
      containers.add(container);
    }
  }

  @Override
  public void handle(ContainerAllocatorEvent containerAllocatorEvent) {
    try {
      eventQueue.put(containerAllocatorEvent);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  protected AMRMClientAsync createAMRMClientAsync() {
    allocListener = new RMCallbackHandler();
    amRMClientAsync = AMRMClientAsync.createAMRMClientAsync(1000,allocListener);
    amRMClientAsync.init(context.getConf());
    return amRMClientAsync;
  }

  public ContainerAllocator(NAppMaster.AppContext context) {
    super(context);
  }

  //TODO
  public void stopContainers() {

  }

  @Override
  public void stop() {
    stopContainers();
    amRMClientAsync.stop();
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
    //setup the ContainerRequest
    //amRMclient.addContainerRequest()
    pendingTasks.add(event.getId());
    //fake code
    allocListener.onContainersAllocated(containers);
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
        ExecutorID id = pendingTasks.get(0);
        id.setContainerId(allocatedContainer.getId());
        if (id instanceof TaskId) {
          TaskEvent taskEvent = new TaskEvent((TaskId)id, TaskEventType.TASK_SETUP);
          taskEvent.setContainer(allocatedContainer);
          LOG.info("post TaskEvent:" + taskEvent + " to " + id);
          eventHandler.handle(taskEvent);
        }
        if (id instanceof WorkerId) {
          WorkerEvent workerEvent = new WorkerEvent((WorkerId)id, WorkerEventType.WORKER_START);
          workerEvent.setContainer(allocatedContainer);
          LOG.info("post WorkerEvent:" + workerEvent + " to " + id);
          eventHandler.handle(workerEvent);
        }
        pendingTasks.remove(0);
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
