package org.apache.hadoop.yarn.applications.narwhal.service;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.applications.narwhal.job.NJobImpl;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerAllocator.RMCallbackHandler;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.doNothing;

public class TestContainerAllocator {

  private ContainerAllocator containerAllocator;
  private AsyncDispatcher dispatcher;
  private ApplicationAttemptId applicationAttemptId;
  private RMCallbackHandler rmCallbackHandler;
  private AMRMClientAsync amRMClientAsync;

  @Before
  public void setup() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, YarnException, IOException {
    YarnConfiguration conf = new YarnConfiguration();

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    JobEventDispatcher jobEventDispatcher = mock(JobEventDispatcher.class);
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    AppContext appContext = mock(AppContext.class);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());

    applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    NJobImpl nJob = mock(NJobImpl.class);
    when(appContext.getJob()).thenReturn(nJob);
    containerAllocator = new ContainerAllocator(appContext);
    rmCallbackHandler = containerAllocator.new RMCallbackHandler();

    mockAMRMClientAsync();
    setAllocatorResource();
  }

  @Test
  public void testSetupAndAddContainerSuccess() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    int mem = 2048;
    int cpus = 2;
    ContainerAllocatorEvent containerAllocatorEvent = prepareContainerAllocatorEvent(mem, cpus, ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
    containerAllocator.processEvent(containerAllocatorEvent);

    Field pendingTasksField = containerAllocator.getClass().getDeclaredField("pendingTasks");
    pendingTasksField.setAccessible(true);
    List<ExecutorID> pendingTasks = (List<ExecutorID>) pendingTasksField.get(containerAllocator);
    assertEquals(pendingTasks.size(), 1);
    assertEquals(pendingTasks.get(0), containerAllocatorEvent.getId());
  }

  @Test
  public void testSetupAndAddContainerExcceedMaxVcores() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    int mem = 2048;
    int cpus = 10;
    ContainerAllocatorEvent containerAllocatorEvent = prepareContainerAllocatorEvent(mem, cpus, ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
    containerAllocator.processEvent(containerAllocatorEvent);

    Field pendingTasksField = containerAllocator.getClass().getDeclaredField("pendingTasks");
    pendingTasksField.setAccessible(true);
    List<ExecutorID> pendingTasks = (List<ExecutorID>) pendingTasksField.get(containerAllocator);
    assertEquals(pendingTasks.size(), 0);
  }

  @Test
  public void testSetupAndAddContainerExcceedMaxMemory() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    int mem = 9216;
    int cpus = 10;
    ContainerAllocatorEvent containerAllocatorEvent = prepareContainerAllocatorEvent(mem, cpus, ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
    containerAllocator.processEvent(containerAllocatorEvent);

    Field pendingTasksField = containerAllocator.getClass().getDeclaredField("pendingTasks");
    pendingTasksField.setAccessible(true);
    List<ExecutorID> pendingTasks = (List<ExecutorID>) pendingTasksField.get(containerAllocator);
    assertEquals(pendingTasks.size(), 0);
  }

  @Test
  public void testAddStartedContainer() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    int mem = 2048;
    int cpus = 10;
    ContainerAllocatorEvent containerAllocatorEvent = prepareContainerAllocatorEvent(mem, cpus, ContainerAllocatorEventType.CONTAINERALLOCATOR_CONTAINER_STARTED);
    containerAllocator.processEvent(containerAllocatorEvent);
    Field startedContainersField = containerAllocator.getClass().getDeclaredField("startedContainers");
    startedContainersField.setAccessible(true);
    ConcurrentHashMap<ContainerId, ExecutorID> startedContainers = (ConcurrentHashMap<ContainerId, ExecutorID>) startedContainersField.get(containerAllocator);
    ExecutorID executorID = startedContainers.get(containerAllocatorEvent.getId().getContainerId());
    assertEquals(executorID, containerAllocatorEvent.getId());
  }

  @Test
  public void testOnContainersAllocatedNoPendingTasks() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    List<Container> mockAllocatedContainers = mockAllocatedContainers(0, 3);
    rmCallbackHandler.onContainersAllocated(mockAllocatedContainers);
    verify(amRMClientAsync, times(3)).releaseAssignedContainer(Matchers.any(ContainerId.class));
  }

  @Test
  public void testOnContainersAllocatedNoMultipleTasks() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    int mem = 8192;
    int cpus = 10;
    ContainerAllocatorEvent containerAllocatorEvent = prepareContainerAllocatorEvent(mem, cpus, ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
    containerAllocator.processEvent(containerAllocatorEvent);

    List<Container> mockAllocatedContainers = mockAllocatedContainers(0, 3);
    rmCallbackHandler.onContainersAllocated(mockAllocatedContainers);
    verify(amRMClientAsync, times(3)).releaseAssignedContainer(Matchers.any(ContainerId.class));
  }

  private List<Container> mockAllocatedContainers(int pendingTaskSize, int allocatedNums) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Field pendingTasksField = containerAllocator.getClass().getDeclaredField("pendingTasks");
    pendingTasksField.setAccessible(true);

    List<ExecutorID> pendingTasks = Collections.synchronizedList(new LinkedList<ExecutorID>());
    for (int i = 0; i < pendingTaskSize; i++) {
      TaskId taskId = new TaskId(new JobId(applicationAttemptId), i + 1);
      pendingTasks.add(taskId);
    }
    pendingTasksField.set(containerAllocator, pendingTasks);

    List<Container> list = new ArrayList<Container>();
    for (int i = 0; i < allocatedNums; i++) {
      Container container = mock(Container.class);
      ContainerId containerId = mock(ContainerId.class);
      when(container.getId()).thenReturn(containerId);
      list.add(container);
    }
    return list;
  }

  @SuppressWarnings({"unchecked"})
  private void mockAMRMClientAsync() throws YarnException, IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    amRMClientAsync = mock(AMRMClientAsync.class);
    doNothing().when(amRMClientAsync).addContainerRequest(Matchers.any(AMRMClient.ContainerRequest.class));

    Field amRMClientAsyncField = containerAllocator.getClass().getDeclaredField("amRMClientAsync");
    amRMClientAsyncField.setAccessible(true);
    amRMClientAsyncField.set(containerAllocator, amRMClientAsync);
  }

  private void setAllocatorResource() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Field maxMemField = containerAllocator.getClass().getDeclaredField("maxMem");
    maxMemField.setAccessible(true);
    maxMemField.set(containerAllocator, 8192);
    Field maxVcoresField = containerAllocator.getClass().getDeclaredField("maxVcores");
    maxVcoresField.setAccessible(true);
    maxVcoresField.set(containerAllocator, 8);
  }

  private ContainerAllocatorEvent prepareContainerAllocatorEvent(int mem, int cpus, ContainerAllocatorEventType eventType) {
    TaskId taskId = new TaskId(new JobId(applicationAttemptId), 1);
    ContainerId containerId = mock(ContainerId.class);
    taskId.setContainerId(containerId);
    ContainerAllocatorEvent containerAllocatorEvent = new ContainerAllocatorEvent(taskId, eventType);
    Resource capability = Resource.newInstance(mem, cpus);
    containerAllocatorEvent.setCapability(capability);
    containerAllocatorEvent.setPriority(0);
    containerAllocatorEvent.setHostname("");
    return containerAllocatorEvent;
  }

  @After
  public void tearDown() {

  }

}
