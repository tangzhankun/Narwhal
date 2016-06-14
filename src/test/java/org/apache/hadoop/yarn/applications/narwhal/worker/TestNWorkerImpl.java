package org.apache.hadoop.yarn.applications.narwhal.worker;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.TaskEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.WorkerEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.TaskEventType;
import org.apache.hadoop.yarn.applications.narwhal.event.WorkerEventType;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestNWorkerImpl {
  private AsyncDispatcher dispatcher;
  private ApplicationAttemptId applicationAttemptId;
  private NWorkerImpl nWork;

  @Before
  public void setup() {
    YarnConfiguration conf = new YarnConfiguration();
    AppContext appContext = mock(AppContext.class);

    dispatcher = new AsyncDispatcher();
    dispatcher.init(conf);
    dispatcher.start();
    TaskEventDispatcher taskEventDispatcher = mock(TaskEventDispatcher.class);
    dispatcher.register(TaskEventType.class, taskEventDispatcher);
    WorkerEventDispatcher workerEventDispatcher = new WorkerEventDispatcher(appContext);
    dispatcher.register(WorkerEventType.class, workerEventDispatcher);
    when(appContext.getEventHandler()).thenReturn(dispatcher.getEventHandler());

    TaskId taskId = mock(TaskId.class);
    String hostname = "localhost";
    String workerCmd = "echo hello";
    String resourceName = "centos_yarn";
    String resourcePath = "centos_yarn_path";
    nWork = new NWorkerImpl(taskId, 1, appContext.getEventHandler(), hostname, workerCmd, resourceName, resourcePath);
  }

  @Test
  public void testScheduleTransition() {

  }

  @Test
  public void testStartTransition() {

  }

  @Test
  public void testCompleteTransitionSuccess() {

  }

  @Test
  public void testCompleteTransitionFail() {

  }

  @After
  public void tearDown() {

  }


}
