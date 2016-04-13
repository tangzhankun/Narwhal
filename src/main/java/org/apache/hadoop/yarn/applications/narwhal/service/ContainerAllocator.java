package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

public class ContainerAllocator extends EventLoop implements EventHandler<ContainerAllocatorEvent>{

  private static final Log LOG = LogFactory.getLog(ContainerAllocator.class);

  @Override
  public void handle(ContainerAllocatorEvent containerAllocatorEvent) {
    try {
      eventQueue.put(containerAllocatorEvent);
    } catch (InterruptedException e) {
      throw new YarnRuntimeException(e);
    }
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
        setupAndAddContainer();
        break;
      case CONTAINERALLOCATOR_DEALLOCATE:
        releaseContainer();
        break;
    }
  }

  private void setupAndAddContainer() {
    LOG.info("allocate container from AM");
    //amRMclient.addContainerRequest()
  }

  private void releaseContainer() {
    LOG.info("release container");
    //amRMClient.releaseAssignedContainer or removeContainerRequest?
  }

}
