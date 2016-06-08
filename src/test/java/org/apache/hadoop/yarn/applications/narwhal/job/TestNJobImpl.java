package org.apache.hadoop.yarn.applications.narwhal.job;

import java.io.IOException;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster.AppContext;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.dispatcher.JobEventDispatcher;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNJobImpl {

  private static YarnConfiguration conf;
  private static TestingServer zkTestServer;
  private static AppContext appContext;
  private static AsyncDispatcher dispatcher;
  private static JobEventDispatcher jobEventDispatcher;

  @BeforeClass
  public static void setup() throws Exception {
    conf = new YarnConfiguration();
    conf.set("hadoop.registry.zk.quorum", "localhost:12181");
    zkTestServer = new TestingServer(12181);

    appContext = mock(AppContext.class);
    jobEventDispatcher = new JobEventDispatcher(appContext);
  }

  @Test
  public void testInitTransition() {
    ApplicationAttemptId applicationAttemptId = mock(ApplicationAttemptId.class);
    when(applicationAttemptId.toString()).thenReturn("appattempt_1465186316357_0001_000001");
    ApplicationId applicationId = mock(ApplicationId.class);
    when(applicationId.toString()).thenReturn("application_1465186316357_0001");
    when(applicationAttemptId.getApplicationId()).thenReturn(applicationId);
    when(appContext.getEventHandler()).thenReturn(jobEventDispatcher);

    NJobImpl nJob = new NJobImpl("Narwhal job", applicationAttemptId, conf, appContext.getEventHandler());
    JobEvent jobEvent = new JobEvent(nJob.getID(), JobEventType.JOB_INIT);

//    String name = "yuqiang-docker";
//    double cpus = 2;
//    double mem = 2048;
    int instances = 10;
//    String cmd = "sleep 15; cat /proc/1/cgroup";
//    String image = "centos_yarn";
//    boolean localImage = false;

    NarwhalConfig narwhalConfig = mock(NarwhalConfig.class);
    when(narwhalConfig.getInstances()).thenReturn(instances);
    jobEvent.setNarwhalConfig(narwhalConfig);

    nJob.handle(jobEvent);
    assertEquals(nJob.getTasks().size(), instances);

  }

  @AfterClass
  public static void tearDown() throws IOException {
    zkTestServer.stop();
  }

}
