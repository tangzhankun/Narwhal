package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.job.NJobImpl;
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

  private class ResourceRecord {
    private ExecutorID executorId;
    private int mem;
    private int vcores;
    private String hosts;
    private AMRMClient.ContainerRequest containerRequest;
    private ContainerAllocatorEvent event;

    public ResourceRecord(AMRMClient.ContainerRequest containerRequest, ContainerAllocatorEvent event,
                          ExecutorID executorId, int mem, int vcores) {
      this.containerRequest = containerRequest;
      this.event = event;
      this.executorId = executorId;
      this.mem = mem;
      this.vcores = vcores;
      this.hosts = event.getHostname();
    }

    public int getMem() {
      return mem;
    }

    public int getVcores() {
      return vcores;
    }

    public String getHosts() {
      return hosts;
    }

    public ContainerAllocatorEvent getEvent() {
      return event;
    }

    public AMRMClient.ContainerRequest getContainerRequest() {
      return containerRequest;
    }
  }

  private AMRMClientAsync amRMClientAsync;

  private AMRMClientAsync.CallbackHandler allocListener;

  private List<ExecutorID> pendingTasks = Collections.synchronizedList(new LinkedList<ExecutorID>());

  private ConcurrentHashMap<ContainerId, ExecutorID> startedContainers =
      new ConcurrentHashMap<>();

  private ConcurrentHashMap<ExecutorID, ResourceRecord> resourceRecords =
      new ConcurrentHashMap<>();

  private int maxMem = 0;
  private int maxVcores = 0;

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
      maxMem = response.getMaximumResourceCapability().getMemory();
      maxVcores = response.getMaximumResourceCapability().getVirtualCores();
      LOG.info("Narwhal AM registered: " + hostname +
          "; Max mems:" + maxMem + "; Max vcores:" + maxVcores);
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
    LOG.info("submit container request to AM");
    Priority pri = Priority.newInstance(event.getPriority());
    Resource capability = event.getCapability();
    String[] nodes = new String[1];
    nodes[0] = event.getHostname();
    if (maxMem < capability.getMemory() |
        maxVcores < capability.getVirtualCores()) {
      LOG.warn("Exceeds cluster max resource for " + event.getId() + ", exiting.");
      eventHandler.handle(new JobEvent(context.getJob().getID(),JobEventType.JOB_ERROR));
      return;
    }
    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability,nodes,null,pri);
    amRMClientAsync.addContainerRequest(request);
    pendingTasks.add(event.getId());
    resourceRecords.put(event.getId(),
        new ResourceRecord(request, event, event.getId(), capability.getMemory(), capability.getVirtualCores()));
  }

  private void releaseContainer() {
    LOG.info("release container");
    //amRMClient.releaseAssignedContainer or removeContainerRequest?
  }

  class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private final Log LOG = LogFactory.getLog(RMCallbackHandler.class);

    //TODO: check node label
    public boolean meetContainerNeeds(ExecutorID executorId, Container allocatedContainer) {
      boolean flag = false;
      ResourceRecord record = resourceRecords.get(executorId);
      if (record == null) {
        LOG.warn("No ResourceRecord found in resource list for" + executorId);
      } else if (record.getMem() <= allocatedContainer.getResource().getMemory() &&
          record.getVcores() <= allocatedContainer.getResource().getVirtualCores()) {
        flag = true;
      } else {
        LOG.info(allocatedContainer.getId() + " don't meet "
            + executorId + "'s needs. allocated(mem:" + allocatedContainer.getResource().getMemory()
            + ", vcores:" + allocatedContainer.getResource().getVirtualCores()
            + ") vs expected(mem:" + record.getMem()
            + ", vcores:" + record.getVcores() + ")");
      }
      return flag;
    }

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
        }
      }
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {
      for (ContainerStatus containerStatus : list) {
        LOG.info("ContainerStatus for containerID: " + containerStatus.getContainerId() +
        ", state: " + containerStatus.getState() + ", exitCode: " + containerStatus.getExitStatus());
        if (containerStatus.getState().equals(ContainerState.COMPLETE)) {
          if (startedContainers.get(containerStatus.getContainerId()) != null) {
            postExecutorCompleteEvent(containerStatus.getContainerId(), containerStatus);
          } else {
            LOG.info("Got response after release container: " + containerStatus.getContainerId());
          }
        }
      }

    }

    @Override
    public void onContainersAllocated(List<Container> list) {
      //given a container, select a matching executor to assign to.
      List<Container> unNeededContainers = new LinkedList<>();
      for (Container allocatedContainer : list) {
        if (pendingTasks.size() == 0) {
          //TODO: FIXME. seems this unreleased container will cause error when stop AMRM client
          LOG.info("All pendingTasks are scheduled, skip. Remaining allocatedContainer size from RM:" + list.size());
          unNeededContainers.add(allocatedContainer);
          continue;
        }
        ExecutorID id = selectExecutor(allocatedContainer);
        if (id == null) {
          LOG.info("No matching executor for this container, add it to be released later: " + allocatedContainer.getId());
          unNeededContainers.add(allocatedContainer);
          continue;
        }
        id.setContainerId(allocatedContainer.getId());
        if (id instanceof TaskId) {
          TaskEvent taskEvent = new TaskEvent((TaskId) id, TaskEventType.TASK_SETUP);
          taskEvent.setContainer(allocatedContainer);
          LOG.info("post TaskEvent:" + taskEvent + " to " + id);
          LOG.info("Allocated " + allocatedContainer.getId() + " to " + id);
          eventHandler.handle(taskEvent);
        }
        if (id instanceof WorkerId) {
          WorkerEvent workerEvent = new WorkerEvent((WorkerId) id, WorkerEventType.WORKER_START);
          workerEvent.setContainer(allocatedContainer);
          LOG.info("post WorkerEvent:" + workerEvent + " to " + id);
          LOG.info("Allocated " + allocatedContainer.getId() + " to " + id);
          eventHandler.handle(workerEvent);
        }
      }
      //release the unneeded container
      if (unNeededContainers.size() > 0) {
        for (Container container : unNeededContainers) {
          amRMClientAsync.releaseAssignedContainer(container.getId());
          LOG.info("Release the unneeded container: " + container.getId());
        }
        reSetupAndAddContainer();
      }
    }

    private void reSetupAndAddContainer() {
      for (ExecutorID executorId : pendingTasks) {
        ResourceRecord record = resourceRecords.get(executorId);
        amRMClientAsync.addContainerRequest(record.getContainerRequest());
        LOG.info("Re-submit containerRequest for: " + executorId);
      }
    }

    //FIFO. Choose a executor to run with this container
    private ExecutorID selectExecutor(Container allocatedContainer) {
      ExecutorID executorId;
      for (Iterator<ExecutorID> it = pendingTasks.iterator(); it.hasNext();) {
        executorId = it.next();
        if (meetContainerNeeds(executorId,allocatedContainer)) {
          it.remove();
          return executorId;
        }
      }
      return null;
    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
      NJobImpl job = (NJobImpl)context.getJob();
      if (job == null) {
        return 0;
      }
      //TODO:Fix me. there should return a non-zero value although there is no task finished
      float actualProgress = (float)job.getFinishedTasksCount()/(float)job.getTasks().size();
      return actualProgress;
    }

    @Override
    public void onError(Throwable throwable) {

    }
  }

}
