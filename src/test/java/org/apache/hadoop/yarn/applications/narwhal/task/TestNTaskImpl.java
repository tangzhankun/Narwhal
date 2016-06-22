package org.apache.hadoop.yarn.applications.narwhal.task;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.WorkerEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.WorkerEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.WorkerEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.applications.narwhal.job.NJobImpl;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerAllocator;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerLauncher;
import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TestNTaskImpl {
  private AsyncDispatcher dispatcher;
  private AppContext appContext;
  private ApplicationAttemptId applicationAttemptId;
  private NTaskImpl nTask;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private JobEventDispatcher jobEventDispatcher;
  private TaskEventDispatcher taskEventDispatcher;
  private WorkerEventDispatcher workerEventDispatcher;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    appContext = mock(AppContext.class);

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    taskEventDispatcher = new TaskEventDispatcher(appContext);
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    containerAllocator = mock(ContainerAllocator.class);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    doNothing().when(containerAllocator).handle(Matchers.any(ContainerAllocatorEvent.class));
    containerLauncher = mock(ContainerLauncher.class);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);
    doNothing().when(containerLauncher).handle(Matchers.any(ContainerLauncherEvent.class));
    workerEventDispatcher = mock(WorkerEventDispatcher.class);
    dispatcher.register(WorkerEventType.class, workerEventDispatcher);
    doNothing().when(workerEventDispatcher).handle(Matchers.any(WorkerEvent.class));
    jobEventDispatcher = new JobEventDispatcher(appContext);
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());
  }

  @Test
  public void testScheduleTransition() {
    createScheduledTask(false);
    verify(containerAllocator, times(1)).handle(Matchers.any(ContainerAllocatorEvent.class));
  }

  @Test
  public void testSetupTransitionNotUsingLocalImage() {
    createAfterSetupTask(false);
  }

  @Test
  public void testSetupTransitionUsingLocalImage() {
    createAfterSetupTask(true);
    verify(workerEventDispatcher, times(1)).handle(Matchers.any(WorkerEvent.class));
  }

  @Test
  public void testLaunchTransition() {
    createAfterSetupTask(false);
    verify(containerLauncher, times(1)).handle(Matchers.any(ContainerLauncherEvent.class));
  }

  @Test
  public void testCompleteTransition() {
    createCompleteTask(false);
  }

  private void createNewTask(boolean isUsingLocalImage) {
    String name = "yuqiang-docker";
    String cmd = "sleep 15; cat /proc/1/cgroup";
    String image = "centos_yarn";
    int cpus = 2;
    int mem = 2048;

    applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);

    NRegistryOperator nRegistryOperator = mock(NRegistryOperator.class);
    doNothing().when(nRegistryOperator).setContainerRecord(Matchers.anyString(), Matchers.anyString(), Matchers.anyString());
    doNothing().when(nRegistryOperator).updateContainer(Matchers.anyString());
    NJobImpl nJob = mock(NJobImpl.class);
    when(appContext.getJob()).thenReturn(nJob);
    JobId jobId = new JobId(ConverterUtils.toApplicationAttemptId(applicationAttemptId.toString()));
    nTask = new NTaskImpl(jobId, 1, appContext.getEventHandler(), cmd, cpus, mem, 0, image, isUsingLocalImage, name, nRegistryOperator);
    when(nJob.getTask(nTask.getID())).thenReturn(nTask);
  }

  private void createScheduledTask(boolean isUsingLocalImage) {
    createNewTask(isUsingLocalImage);
    TaskEvent taskEvent = new TaskEvent(nTask.getID(), TaskEventType.TASK_SCHEDULE);
    nTask.handle(taskEvent);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    assertEquals(nTask.getStatus(), TaskState.SCHEDULED);
  }

  private void createAfterSetupTask(boolean isUsingLocalImage) {
    createScheduledTask(isUsingLocalImage);
    TaskEvent taskEvent = new TaskEvent(nTask.getID(), TaskEventType.TASK_SETUP);
    Container container = mock(Container.class);
    ContainerId containerId = mock(ContainerId.class);
    when(container.getId()).thenReturn(containerId);
    when(containerId.toString()).thenReturn("containerId");
    NodeId nodeId = mock(NodeId.class);
    when(container.getNodeId()).thenReturn(nodeId);
    when(nodeId.getHost()).thenReturn("localhost");
    when(nodeId.getPort()).thenReturn(1234);
    taskEvent.setContainer(container);
    nTask.handle(taskEvent);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    if (isUsingLocalImage) {
      assertEquals(nTask.getStatus(), TaskState.SCHEDULED);
    } else {
      assertEquals(nTask.getStatus(), TaskState.RUNNING);
    }
  }

  private void createCompleteTask(boolean isUsingLocalImage) {
    createAfterSetupTask(isUsingLocalImage);
    TaskEvent taskEvent = new TaskEvent(nTask.getID(), TaskEventType.TASK_COMPLETED);
    Container container = mock(Container.class);
    ContainerId containerId = mock(ContainerId.class);
    when(container.getId()).thenReturn(containerId);
    when(containerId.toString()).thenReturn("containerId");
    taskEvent.setContainer(container);
    nTask.handle(taskEvent);
    assertEquals(nTask.getStatus(), TaskState.SUCCEED);
  }

  @After
  public void tearDown() throws IOException {

  }

}
