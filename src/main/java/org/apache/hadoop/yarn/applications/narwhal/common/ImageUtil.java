package org.apache.hadoop.yarn.applications.narwhal.common;

/**
 *
 */
public class ImageUtil {
  public static String getFSFileName(String dockerImageName) {
    String ns = dockerImageName.replace(":","_");
    return ns + ".tar";
  }

  public static String getTempDir() {
    return "/tmp/";
  }

  public static String getFSFilePathSuffix(String appName, String appId, String fileName) {
    return appName + "/" + appId + "/" + fileName;
  }

}
