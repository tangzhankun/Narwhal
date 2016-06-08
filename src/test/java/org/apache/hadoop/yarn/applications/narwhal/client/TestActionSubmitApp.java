package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.client.ActionSubmitApp;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.applications.narwhal.util.MiniClusterCreator;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.MiniYARNCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestActionSubmitApp {

  private static MiniYARNCluster yarnCluster;
  private static YarnConfiguration conf;

  private static int numResourceManagers = 1;
  private static int numNodeManagers = 1;
  private static int numLocalDirs = 1;
  private static int numLogDirs = 1;

  private String NAppMasterJar = JarFinder.getJar(NAppMaster.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @BeforeClass
  public static void setup() throws IOException {
    yarnCluster = MiniClusterCreator.createYarnCluster(TestActionSubmitApp.class.getSimpleName(), numResourceManagers, numNodeManagers, numLocalDirs, numLogDirs);
    conf = new YarnConfiguration();
    yarnCluster.init(conf);
    yarnCluster.start();

  }

  @Test
  public void testInitWithRightParameters() throws ParseException {
    ClientAction action = new ActionSubmitApp(conf);
    String[] args = {
            "-jar", NAppMasterJar, "-configFile", "target/test-classes/artifact.json"
    };
    boolean inited = action.init(args);
    assertTrue(inited);
  }

  @Test
  public void testInitWithWrongParameters() throws ParseException {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No config file specified");

    ClientAction action = new ActionSubmitApp(conf);
    String[] argsWithJar = {
            "-jar", NAppMasterJar
    };
    action.init(argsWithJar);

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("No jar file specified for application master");

    String[] argsWithConfig = {
            "-configFile", "target/test-classes/artifact.json"
    };
    action.init(argsWithConfig);
  }

  @Test
  public void testInitNoSuchJsonFile() throws ParseException {

    ClientAction action = new ActionSubmitApp(conf);
    String[] args = {
            "-jar", NAppMasterJar, "-configFile", "target/test-classes/wrong.json"
    };
    boolean inited = action.init(args);
    assertFalse(inited);
  }

  @Test
  public void testSubmitAppContext() throws YarnException, IOException, InterruptedException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    ActionSubmitApp action = new ActionSubmitApp(yarnCluster.getConfig());

    String name = "yuqiang-docker";
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    String cmd = "sleep 15; cat /proc/1/cgroup";
    String image = "centos_yarn";
    boolean localImage = false;

    NarwhalConfig narwhalConfig = mock(NarwhalConfig.class);
    when(narwhalConfig.getName()).thenReturn(name);
    when(narwhalConfig.isEngineLocalImage()).thenReturn(localImage);
    when(narwhalConfig.getEngineImage()).thenReturn(image);

    Field narwhalConfigField = action.getClass().getDeclaredField("narwhalConfig");
    narwhalConfigField.setAccessible(true);
    narwhalConfigField.set(action, narwhalConfig);
    Field appMasterJarField = action.getClass().getDeclaredField("appMasterJar");
    appMasterJarField.setAccessible(true);
    appMasterJarField.set(action, NAppMasterJar);

    //container-executor error here for mini cluster
    ApplicationId submitAppContext = action.submitAppContext();
    assertNotNull(submitAppContext);

  }

  @Test
  public void testMonitorApplicartionSuccess() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, YarnException, IOException {
    ActionSubmitApp action = new ActionSubmitApp(conf);

    ApplicationId appId = mock(ApplicationId.class);
    when(appId.getId()).thenReturn(1);

    YarnClient yarnClient = mock(YarnClient.class);
    ApplicationReport report = mock(ApplicationReport.class);
    when(report.getDiagnostics()).thenReturn("fake diagnostics");
    when(report.getQueue()).thenReturn("fake queue");
    when(report.getProgress()).thenReturn(0.5f);
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
    when(report.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.SUCCEEDED);
    when(yarnClient.getApplicationReport(appId)).thenReturn(report);

    Field yarnClientField = action.getClass().getDeclaredField("yarnClient");
    yarnClientField.setAccessible(true);
    yarnClientField.set(action, yarnClient);

    boolean result = action.monitorApplicartion(appId);
    assertTrue(result);
  }

  @AfterClass
  public static void tearDown() {
    if (yarnCluster != null) {
      yarnCluster.stop();
    }
  }

}
