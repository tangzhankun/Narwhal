package org.apache.hadoop.yarn.applications.narwhal.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

/**
 *
 */
public class ContainerLauncher extends EventLoop implements EventHandler<ContainerLauncherEvent> {

  private static final Log LOG = LogFactory.getLog(ContainerLauncher.class);

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
        launchContainer();
        break;
      case CONTAINERLAUNCHER_COMPLETED:
        completeContainer();
        break;
      case CONTAINERLAUNCHER_CLEANUP:
        cleanupContainer();
        break;
    }
  }

  private void launchContainer() {
    LOG.info("start container");
    //nmClientAsync.startContainerAsync()
  }

  private void completeContainer() {
    LOG.info("complete container");
  }

  private void cleanupContainer() {
    LOG.info("stop container");
    //nmClientAysnc.stopContainerAsync()
  }

}
