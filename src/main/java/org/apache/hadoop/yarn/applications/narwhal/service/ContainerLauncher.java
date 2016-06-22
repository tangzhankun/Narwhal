package org.apache.hadoop.yarn.applications.narwhal.service;

import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class ContainerLauncher extends EventLoop implements EventHandler<ContainerLauncherEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerLauncher.class);

  private NMClientAsync nmClientAsync;

  private NMClientAsync.CallbackHandler launchListener;

  private ConcurrentHashMap<ContainerId, ExecutorID> scheduledContainers =
      new ConcurrentHashMap<>();


  public ContainerLauncher(NAppMaster.AppContext context) {
    super(context);
  }

  //TODO
  public void stopContainers() {

  }

  @Override
  public void stop() {
    stopContainers();
    nmClientAsync.stop();
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

  private ContainerLaunchContext buildContainerContext(String cmd, String image,
                                                       boolean useDocker, Map<String, LocalResource> localResources ) {
    ContainerLaunchContext ctx = null;
    try {
      //env
      Map<String, String> env = new HashedMap();
      if (useDocker) {
        env.put("YARN_CONTAINER_RUNTIME_TYPE", "docker");
        env.put("YARN_CONTAINER_RUNTIME_DOCKER_IMAGE", image);
      }
      List<String> commands = new ArrayList<>();
      //cmd
      Vector<CharSequence> vargs = new Vector<>(5);
      vargs.add("(" + cmd + ")");
      vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
      vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");
      StringBuilder command = new StringBuilder();
      for (CharSequence str : vargs) {
        command.append(str).append(" ");
      }
      commands.add(command.toString());
      //tokens
      Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      //ctx
      ctx = ContainerLaunchContext.newInstance(
          localResources, env, commands, null, allTokens.duplicate(), null
      );
    } catch (IOException e) {
      e.printStackTrace();
    }
    return ctx;
  }

  private void launchContainer(ContainerLauncherEvent event) {
    LOG.info("start container");
    String userCmd = event.getUserCmd();
    ContainerLaunchContext ctx = null;
    if (event.getId() instanceof TaskId) {
       ctx = buildContainerContext(userCmd, event.getDockerImageName(), true, null);
    } else {
      Map<String, LocalResource> localResources = new HashMap<>();
      String resourceFileName = event.getResourceFileName();
      String resourcePath = event.getResourceFilePath();
      if (resourcePath != "") {
        FileSystem fs = null;
        try {
          fs = FileSystem.get(new YarnConfiguration());
          Path dst = new Path(fs.getHomeDirectory(), resourcePath);
          boolean exists = fs.exists(dst);
          if(exists) {
            FileStatus scFileStatus = fs.getFileStatus(dst);
            LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromURI(dst.toUri()), LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
                scFileStatus.getModificationTime());
            localResources.put(resourceFileName, scRsrc);          	
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      ctx = buildContainerContext(userCmd, null, false, localResources);
    }
    if (ctx == null) {
      LOG.info("ContainerLaunchContext is null");
    } else {
      if (event.getContainer() == null) {
        LOG.info("Container is null:" + event.getId());
      }
      LOG.info(event.getId() + " used container " + event.getContainer().getId());
      nmClientAsync.startContainerAsync(event.getContainer(), ctx);
      scheduledContainers.put(event.getId().getContainerId(), event.getId());
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
      LOG.info("NM - Container: " + containerId + " started");
      Iterator<ContainerId> it = scheduledContainers.keySet().iterator();
      while (it.hasNext()) {
        ContainerId scheduledContainerId = it.next();
        if (scheduledContainerId.equals(containerId)) {
          ExecutorID executorID = scheduledContainers.get(scheduledContainerId);
          //post event to ContainerAllocator to tell it one container has started
          ContainerAllocatorEvent event = new ContainerAllocatorEvent(executorID,
              ContainerAllocatorEventType.CONTAINERALLOCATOR_CONTAINER_STARTED);
          eventHandler.handle(event);
          //remove from schedulerContainer list
          it.remove();
        }
      }
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
      LOG.info("NM - Container: " + containerId + "status received : " + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
      LOG.info("NM - Container" + containerId + " stopped");
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable throwable) {
      LOG.info("NM - start container" + containerId + " encountered error");
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable throwable) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable throwable) {
      LOG.info("NM - stop container" + containerId + " encountered error");
    }
  }

}
