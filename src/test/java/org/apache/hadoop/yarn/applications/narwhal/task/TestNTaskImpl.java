package org.apache.hadoop.yarn.applications.narwhal.task;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerLauncherEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
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
  private ApplicationAttemptId applicationAttemptId;
  private NTaskImpl nTask;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    AppContext appContext = mock(AppContext.class);

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher(appContext);
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    containerAllocator = mock(ContainerAllocator.class);
    dispatcher.register(ContainerAllocatorEventType.class, containerAllocator);
    doNothing().when(containerAllocator).handle(Matchers.any(ContainerAllocatorEvent.class));
    containerLauncher = mock(ContainerLauncher.class);
    dispatcher.register(ContainerLauncherEventType.class, containerLauncher);
    doNothing().when(containerLauncher).handle(Matchers.any(ContainerLauncherEvent.class));

    applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());

    String name = "yuqiang-docker";
    String cmd = "sleep 15; cat /proc/1/cgroup";
    String image = "centos_yarn";
    int cpus = 2;
    int mem = 2048;
    boolean localImage = false;

    NRegistryOperator nRegistryOperator = mock(NRegistryOperator.class);
    doNothing().when(nRegistryOperator).setContainerRecord(Matchers.anyString(), Matchers.anyString(), Matchers.anyString());
    doNothing().when(nRegistryOperator).updateContainer(Matchers.anyString());
    JobId jobId = new JobId(ConverterUtils.toApplicationAttemptId(applicationAttemptId.toString()));
    nTask = new NTaskImpl(jobId, 1, appContext.getEventHandler(), cmd, cpus, mem, 0, image, localImage, name, nRegistryOperator);
  }

  @Test
  public void testScheduleTransition() {
    TaskEvent taskEvent = new TaskEvent(nTask.getID(), TaskEventType.TASK_SCHEDULE);
    nTask.handle(taskEvent);
    assertEquals(nTask.getStatus(), TaskState.SCHEDULED);
    verify(containerAllocator, times(1)).handle(Matchers.any(ContainerAllocatorEvent.class));
  }

  @Test
  public void testSetupTransitionNotUsingLocalImage() {
    TaskEvent taskEvent = new TaskEvent(nTask.getID(), TaskEventType.TASK_SETUP);
    nTask.handle(taskEvent);
    assertEquals(nTask.getStatus(), TaskState.READY);
//  	verify(containerLauncher, times(1)).handle(Matchers.any(ContainerLauncherEvent.class));
  }

  @Test
  public void testSetupTransitionUsingLocalImage() {

  }

  @Test
  public void testLaunchTransition() {

  }

  @Test
  public void testCompleteTransition() {

  }


  @After
  public void tearDown() throws IOException {

  }

}
