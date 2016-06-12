package org.apache.hadoop.yarn.applications.narwhal.service;

import java.lang.reflect.Field;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.ContainerAllocatorEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.applications.narwhal.task.ExecutorID;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;

public class TestContainerAllocator {

  private static YarnConfiguration conf;
  private static AppContext appContext;
  private static ContainerAllocator containerAllocator;

  @BeforeClass
  public static void setup() {
    conf = new YarnConfiguration();
    appContext = mock(AppContext.class);
    when(appContext.getConf()).thenReturn(conf);

    containerAllocator = new ContainerAllocator(appContext);
  }

  @Test
  public void testSetupAndAddContainer() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    AMRMClientAsync amRMClientAsync = mock(AMRMClientAsync.class);
    doNothing().when(amRMClientAsync).addContainerRequest(Matchers.any(AMRMClient.ContainerRequest.class));

    Field maxMemField = containerAllocator.getClass().getDeclaredField("maxMem");
    maxMemField.setAccessible(true);
    maxMemField.set(containerAllocator, 8192);
    Field maxVcoresField = containerAllocator.getClass().getDeclaredField("maxVcores");
    maxVcoresField.setAccessible(true);
    maxVcoresField.set(containerAllocator, 8);
    Field amRMClientAsyncField = containerAllocator.getClass().getDeclaredField("amRMClientAsync");
    amRMClientAsyncField.setAccessible(true);
    amRMClientAsyncField.set(containerAllocator, amRMClientAsync);

    TaskId taskId = new TaskId(new JobId(applicationAttemptId), 1);
    int mem = 2048;
    int cpu = 2;
    ContainerAllocatorEvent containerAllocatorEvent = new ContainerAllocatorEvent(taskId, ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
    Resource capability = Resource.newInstance(mem, cpu);
    containerAllocatorEvent.setCapability(capability);
    containerAllocatorEvent.setPriority(0);
    containerAllocatorEvent.setHostname("");

    containerAllocator.processEvent(containerAllocatorEvent);

    Field pendingTasksField = containerAllocator.getClass().getDeclaredField("pendingTasks");
    pendingTasksField.setAccessible(true);
    List<ExecutorID> pendingTasks = (List<ExecutorID>) pendingTasksField.get(containerAllocator);
    assertEquals(pendingTasks.size(), 1);
    assertEquals(pendingTasks.get(0), containerAllocatorEvent.getId());

  }

  @AfterClass
  public static void tearDown() {

  }

}
