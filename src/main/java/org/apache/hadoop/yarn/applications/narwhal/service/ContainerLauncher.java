package org.apache.hadoop.yarn.applications.narwhal.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ContainerLauncher extends EventLoop implements EventHandler<ContainerLauncherEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerLauncher.class);

  private NMClientAsync nmClientAsync;

  private NMClientAsync.CallbackHandler launchListener;

  private ConcurrentHashMap<ContainerId, TaskId> scheduledContainers =
      new ConcurrentHashMap<>();

  private ConcurrentHashMap<ContainerId, TaskId> startdContainers =
      new ConcurrentHashMap<>();

  private ConcurrentHashMap<ContainerId, TaskId> stoppedContainers =
      new ConcurrentHashMap<>();

  public ContainerLauncher(NAppMaster.AppContext context) {
    super(context);
  }

  @Override
  public void handle(ContainerLauncherEvent containerLauncherEvent) {
    try {
      eventQueue.put(containerLauncherEvent);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
  }

  @Override
  public void processEvent(AbstractEvent event) {
    ContainerLauncherEvent CLEvent = (ContainerLauncherEvent)event;
    LOG.info("Processing the event " + CLEvent);
    switch (CLEvent.getType()) {
      case CONATAINERLAUNCHER_LAUNCH:
        launchContainer(CLEvent);
        break;
      case CONTAINERLAUNCHER_COMPLETED:
        completeContainer();
        break;
      case CONTAINERLAUNCHER_CLEANUP:
        cleanupContainer();
        break;
    }
  }

  @Override
  public void startClientAsync() {
    createNMClientAsync().start();
  }

  protected NMClientAsync createNMClientAsync() {
    launchListener = new NMCallback();
    nmClientAsync = NMClientAsync.createNMClientAsync(launchListener);
    nmClientAsync.init(context.getConf());
    return nmClientAsync;
  }

  private void launchContainer(ContainerLauncherEvent event) {
    LOG.info("start container");
    //nmClientAsync.startContainerAsync()
    scheduledContainers.put(event.getId().getContainerId(), event.getId());
    //fake code
    for (int i = 1; i < 6; i++) {
      ApplicationId applicationId = ApplicationId.newInstance(i, i);
      ApplicationAttemptId applicationAttemptId = ApplicationAttemptId.newInstance(applicationId, i);
      ContainerId containerId = ContainerId.newContainerId(applicationAttemptId, i);
      Container container = Container.newInstance(containerId, NodeId.newInstance("host", 5000),
          "host:80", Resource.newInstance(1024, 1), Priority.newInstance(0), null);
      launchListener.onContainerStarted(containerId,null);
    }


  }

  private void completeContainer() {
    LOG.info("complete container");
  }

  private void cleanupContainer() {
    LOG.info("stop container");
    //nmClientAysnc.stopContainerAsync()
  }

  class NMCallback implements NMClientAsync.CallbackHandler {

    private final Log LOG = LogFactory.getLog(NMCallback.class);

    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> map) {
      Iterator<ContainerId> it = scheduledContainers.keySet().iterator();
      while (it.hasNext()) {
        ContainerId scheduledContainerId = it.next();
        if (scheduledContainerId.equals(containerId)) {
          startdContainers.put(containerId, scheduledContainers.get(scheduledContainerId));
          //remove from schedulerContainer list
          it.remove();

          //fake code
          launchListener.onContainerStopped(containerId);
        }
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.info("Container stopped");
      Iterator<ContainerId> it = startdContainers.keySet().iterator();
      while (it.hasNext()) {
        ContainerId startedContainerId = it.next();
        TaskId taskId = startdContainers.get(startedContainerId);
        if (startedContainerId.equals(containerId)) {
          eventHandler.handle(new TaskEvent(taskId, TaskEventType.TASK_COMPLETED));
          //remove from startedContainer list
          it.remove();
        }
      }
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {

    }
  }

}
