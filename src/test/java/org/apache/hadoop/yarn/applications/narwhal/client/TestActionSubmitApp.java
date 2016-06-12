package org.apache.hadoop.yarn.applications.narwhal.client;

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.commons.cli.ParseException;
import org.apache.hadoop.util.JarFinder;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.client.ActionSubmitApp;
import org.apache.hadoop.yarn.applications.narwhal.config.NarwhalConfig;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.withSettings;

public class TestActionSubmitApp {

  private static YarnClient yarnClient;
  private static NarwhalConfig narwhalConfig;
  private static ApplicationId appId;
  private static ApplicationReport report;
  private ActionSubmitApp action;
  private String NAppMasterJar = JarFinder.getJar(NAppMaster.class);

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Before
  public void setup() throws IOException, YarnException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    mockNarwhalConfig();
    mockYarnClient();
    YarnConfiguration conf = new YarnConfiguration();
    action = new ActionSubmitApp(conf);
    setActionField(action);
  }

  @Test
  public void testInitWithRightParameters() throws ParseException {
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
    String[] args = {
            "-jar", NAppMasterJar, "-configFile", "target/test-classes/wrong.json"
    };
    boolean inited = action.init(args);
    assertFalse(inited);
  }

  @Test
  public void testSubmitAppContext() throws YarnException, IOException, InterruptedException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    ApplicationId appIdSubmited = action.submitAppContext();
    assertEquals(appIdSubmited, appId);
  }

  @Test
  public void testMonitorApplicartionSuccess() throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException, YarnException, IOException {
    //state is FINISHED, dsState is SUCCEEDED
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
    when(report.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.SUCCEEDED);
    boolean result = action.monitorApplicartion(appId);
    assertTrue(result);
  }

  @Test
  public void testMonitorApplicartionFinalUnSuccess() throws YarnException, IOException {
    //state is FINISHED, dsState is UNDEFINED
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FINISHED);
    when(report.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    boolean result = action.monitorApplicartion(appId);
    assertFalse(result);
  }

  @Test
  public void testMonitorApplicartionFailed() throws YarnException, IOException {
    //state is FAILED
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.FAILED);
    when(report.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    boolean result = action.monitorApplicartion(appId);
    assertFalse(result);
  }

  @Test
  public void testMonitorApplicartionKilled() throws YarnException, IOException {
    //state is KILLED
    when(report.getYarnApplicationState()).thenReturn(YarnApplicationState.KILLED);
    when(report.getFinalApplicationStatus()).thenReturn(FinalApplicationStatus.UNDEFINED);
    boolean result = action.monitorApplicartion(appId);
    assertFalse(result);
  }

  private static void mockNarwhalConfig() {
    String name = "yuqiang-docker";
    double cpus = 2;
    double mem = 2048;
    int instances = 10;
    String cmd = "sleep 15; cat /proc/1/cgroup";
    String image = "centos_yarn";
    boolean localImage = false;

    narwhalConfig = mock(NarwhalConfig.class, withSettings().serializable());
    when(narwhalConfig.getName()).thenReturn(name);
    when(narwhalConfig.getCpus()).thenReturn(cpus);
    when(narwhalConfig.getMem()).thenReturn(mem);
    when(narwhalConfig.getInstances()).thenReturn(instances);
    when(narwhalConfig.getCmd()).thenReturn(cmd);
    when(narwhalConfig.isEngineLocalImage()).thenReturn(localImage);
    when(narwhalConfig.getEngineImage()).thenReturn(image);
  }

  private static void mockYarnClient() throws YarnException, IOException {
    yarnClient = mock(YarnClient.class);
    doNothing().when(yarnClient).start();
    YarnClientApplication app = mock(YarnClientApplication.class);
    when(yarnClient.createApplication()).thenReturn(app);
    GetNewApplicationResponse appResponse = mock(GetNewApplicationResponse.class);
    when(app.getNewApplicationResponse()).thenReturn(appResponse);
    ApplicationSubmissionContext appContext = mock(ApplicationSubmissionContext.class);
    when(app.getApplicationSubmissionContext()).thenReturn(appContext);
    appId = mock(ApplicationId.class);
    when(appContext.getApplicationId()).thenReturn(appId);
    doNothing().when(appContext).setApplicationName(Matchers.anyString());
    report = mock(ApplicationReport.class);
    when(yarnClient.getApplicationReport(appId)).thenReturn(report);
    when(appId.getId()).thenReturn(1);
    when(appId.toString()).thenReturn("application_1465186316357_0001");
    when(report.getDiagnostics()).thenReturn("fake diagnostics");
    when(report.getQueue()).thenReturn("fake queue");
    when(report.getProgress()).thenReturn(0.5f);
  }

  private void setActionField(Object action) throws NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
    Field narwhalConfigField = action.getClass().getDeclaredField("narwhalConfig");
    narwhalConfigField.setAccessible(true);
    narwhalConfigField.set(action, narwhalConfig);
    Field appMasterJarField = action.getClass().getDeclaredField("appMasterJar");
    appMasterJarField.setAccessible(true);
    appMasterJarField.set(action, NAppMasterJar);
    Field yarnClientField = action.getClass().getDeclaredField("yarnClient");
    yarnClientField.setAccessible(true);
    yarnClientField.set(action, yarnClient);
  }

  @After
  public void tearDown() {

  }

}
