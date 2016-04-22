package org.apache.hadoop.yarn.applications.narwhal.event;


public enum ContainerAllocatorEventType {
  CONTAINERALLOCATOR_REQEUST,
  CONTAINERALLOCATOR_DEALLOCATE,
  //currently the NM's onContainerStopped don't triggered automatically
  //so we use RM's onContainerCompleted to trigger the Task or work complete
  CONTAINERALLOCATOR_CONTAINER_STARTED
}
