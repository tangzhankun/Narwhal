package org.apache.hadoop.yarn.applications.narwhal.worker;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.WorkerEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.WorkerEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.WorkerEventType;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerAllocator;
import org.apache.hadoop.yarn.applications.narwhal.service.ContainerLauncher;
import org.apache.hadoop.yarn.applications.narwhal.state.WorkerState;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

public class TestNWorkerImpl {
  private AsyncDispatcher dispatcher;
  private NWorkerImpl nWorker;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private TaskEventDispatcher taskEventDispatcher;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    AppContext appContext = mock(AppContext.class);

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    taskEventDispatcher = mock(TaskEventDispatcher.class);
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    WorkerEventDispatcher workerEventDispatcher = new WorkerEventDispatcher(appContext);
    dispatcher.register(WorkerEventType.class, workerEventDispatcher);
    containerAllocator = mock(ContainerAllocator.class);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    doNothing().when(containerAllocator).handle(Matchers.any(ContainerAllocatorEvent.class));
    containerLauncher = mock(ContainerLauncher.class);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);
    doNothing().when(containerLauncher).handle(Matchers.any(ContainerLauncherEvent.class));
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());

    TaskId taskId = mock(TaskId.class);
    when(taskId.toString()).thenReturn("taskId");
    String hostname = "localhost";
    String workerCmd = "echo hello";
    String resourceName = "centos_yarn";
    String resourcePath = "centos_yarn_path";
    nWorker = new NWorkerImpl(taskId, 1, appContext.getEventHandler(), hostname, workerCmd, resourceName, resourcePath);
  }

  @Test
  public void testScheduleTransition() {
    createScheduledWorker();
    verify(containerAllocator, times(1)).handle(Matchers.any(ContainerAllocatorEvent.class));
    ;
  }

  @Test
  public void testStartTransition() {
    createStartedWorker();
    verify(containerLauncher, times(1)).handle(Matchers.any(ContainerLauncherEvent.class));
    ;
  }

  @Test
  public void testCompleteTransitionSuccess() {
    createCompletedWorker(0);
    verify(taskEventDispatcher, times(1)).handle(Matchers.any(TaskEvent.class));
    ;
  }

  @Test
  public void testCompleteTransitionFail() {
    createCompletedWorker(1);
  }

  private void createScheduledWorker() {
    WorkerEvent workerEvent = new WorkerEvent(nWorker.getID(), WorkerEventType.WORKER_SCHEDULE);
    nWorker.handle(workerEvent);
    sleep(1000);
    assertEquals(nWorker.getStatus(), WorkerState.SCHEDULED);
  }

  private void createStartedWorker() {
    createScheduledWorker();
    WorkerEvent workerEvent = new WorkerEvent(nWorker.getID(), WorkerEventType.WORKER_START);
    Container container = mock(Container.class);
    workerEvent.setContainer(container);
    nWorker.handle(workerEvent);
    sleep(1000);
    assertEquals(nWorker.getStatus(), WorkerState.RUNNING);
  }

  private void createCompletedWorker(int returnValue) {
    createStartedWorker();
    WorkerEvent workerEvent = new WorkerEvent(nWorker.getID(), WorkerEventType.WORKER_COMPLETED);
    ContainerStatus containerStatus = mock(ContainerStatus.class);
    when(containerStatus.getExitStatus()).thenReturn(returnValue);
    workerEvent.setContainerStatus(containerStatus);
    nWorker.handle(workerEvent);
    sleep(1000);
    if (returnValue == 0) {
      assertEquals(nWorker.getStatus(), WorkerState.SUCCEED);
    } else {
      assertEquals(nWorker.getStatus(), WorkerState.FAILED);
    }
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() {

  }


}
