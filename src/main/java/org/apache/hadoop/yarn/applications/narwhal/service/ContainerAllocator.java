package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ContainerAllocator extends EventLoop implements EventHandler<ContainerAllocatorEvent>{

  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  private AMRMClientAsync amRMClientAsync;

  private AMRMClientAsync.CallbackHandler allocListener;

  private List<ExecutorID> pendingTasks = Collections.synchronizedList(new LinkedList<ExecutorID>());

  private ConcurrentHashMap<ContainerId, ExecutorID> startedContainers =
      new ConcurrentHashMap<>();

  private int allocatedContainerNum = 0;
  private int completedCounterNum = 0;
  private int startedContainerNum = 0;
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

  public void addStartedContainer(ContainerAllocatorEvent event) {
    startedContainers.put(event.getId().getContainerId(), event.getId());
    startedContainerNum++;
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
      case CONTAINERALLOCATOR_CONTAINER_STARTED:
        addStartedContainer(CAEvent);
        break;
    }
  }

  @Override
  public void startClientAsync(){
    createAMRMClientAsync().start();
    //register with RM
    String hostname = NetUtils.getHostname();
    try {
      RegisterApplicationMasterResponse response = amRMClientAsync.registerApplicationMaster(
        hostname,-1,"");
      LOG.info("Narwhal AM registered: " + hostname);
    } catch (YarnException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public void unregisterAM(FinalApplicationStatus appStatus, String msg, String appTrackingUrl) {
    try {
      amRMClientAsync.unregisterApplicationMaster(appStatus, msg, appTrackingUrl);
      LOG.info("Narwhal AM unregistered");
    } catch (YarnException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void setupAndAddContainer(ContainerAllocatorEvent event) {
    LOG.info("allocate container from AM");
    //setup the ContainerRequest
    //amRMclient.addContainerRequest()
    Priority pri = Priority.newInstance(event.getPriority());
    Resource capability = event.getCapability();
    String[] nodes = new String[1];
    nodes[0] = event.getHostname();
    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability,nodes,null,pri);
    amRMClientAsync.addContainerRequest(request);
    pendingTasks.add(event.getId());
  }

  private void releaseContainer() {
    LOG.info("release container");
    //amRMClient.releaseAssignedContainer or removeContainerRequest?
  }

  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private final Log LOG = LogFactory.getLog(RMCallbackHandler.class);


    public void postExecutorCompleteEvent(ContainerId containerId, ContainerStatus containerStatus) {
      Iterator<ContainerId> it = startedContainers.keySet().iterator();
      while (it.hasNext()) {
        ContainerId startedContainerId = it.next();
        ExecutorID id = startedContainers.get(startedContainerId);
        if (startedContainerId.equals(containerId)) {
          if (id instanceof TaskId) {
            TaskEvent taskEvent = new TaskEvent((TaskId) id, TaskEventType.TASK_COMPLETED);
            taskEvent.setContainerStatus(containerStatus);
            eventHandler.handle(taskEvent);
          } else if (id instanceof WorkerId) {
            WorkerEvent workerEvent = new WorkerEvent((WorkerId)id, WorkerEventType.WORKER_COMPLETED);
            workerEvent.setContainerStatus(containerStatus);
            eventHandler.handle(workerEvent);
          }
          //remove from startedContainer list
          it.remove();
          startedContainerNum--;
        }
      }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {
      for (ContainerStatus containerStatus : list) {
        LOG.info("ContainerStatus for containerID: " + containerStatus.getContainerId() +
        ", state: " + containerStatus.getState() + ", exitCode: " + containerStatus.getExitStatus());
        if (containerStatus.getState().equals(ContainerState.COMPLETE)) {
          postExecutorCompleteEvent(containerStatus.getContainerId(), containerStatus);
          completedCounterNum++;
        }
      }

    }

    @Override
    public void onContainersAllocated(List<Container> list) {
      if (pendingTasks.size() == 0) {
        LOG.info("No container request in pending queue, so skip these container:");
        //TODO: zhankun, we should cancel/release this container
        for (Container allocatedContainer : list) {
          LOG.info("Got " + allocatedContainer.getId() + " from RM, weired");
        }
        return;
      }
      //TODO: Zhankun. no assign policy now, just FIFO. should check the resource/node label and then assign containers to tasks
      for (Container allocatedContainer : list) {
        ExecutorID id = pendingTasks.get(0);
        id.setContainerId(allocatedContainer.getId());
        if (id instanceof TaskId) {
          TaskEvent taskEvent = new TaskEvent((TaskId)id, TaskEventType.TASK_SETUP);
          taskEvent.setContainer(allocatedContainer);
          LOG.info("post TaskEvent:" + taskEvent + " to " + id);
          LOG.info("Allocated " + allocatedContainer.getId() + " to " + id);
          eventHandler.handle(taskEvent);
        }
        if (id instanceof WorkerId) {
          WorkerEvent workerEvent = new WorkerEvent((WorkerId)id, WorkerEventType.WORKER_START);
          workerEvent.setContainer(allocatedContainer);
          LOG.info("post WorkerEvent:" + workerEvent + " to " + id);
          LOG.info("Allocated " + allocatedContainer.getId() + " to " + id);
          eventHandler.handle(workerEvent);
        }
        pendingTasks.remove(0);
        allocatedContainerNum++;
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
      if (allocatedContainerNum == 0) {
        return 0;
      }
      //TODO: this is an estimate progress
      return (float) ((completedCounterNum)/allocatedContainerNum);
    }

    @Override
    public void onError(Throwable throwable) {

    }
  }

}
