package org.apache.hadoop.yarn.applications.narwhal.util;

import org.apache.hadoop.yarn.server.MiniYARNCluster;

public class MiniClusterCreator {

  private MiniClusterCreator() {
  }

  public static MiniYARNCluster createYarnCluster(String testName, int numResourceManagers, int numNodeManagers, int numLocalDirs, int numLogDirs) {
    MiniYARNCluster miniCluster = new MiniYARNCluster(testName, numLogDirs, numLogDirs, numLogDirs, numLogDirs);
    return miniCluster;
  }

}
