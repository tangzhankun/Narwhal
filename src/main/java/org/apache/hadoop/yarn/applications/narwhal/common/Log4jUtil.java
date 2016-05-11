package org.apache.hadoop.yarn.applications.narwhal.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.PropertyConfigurator;

public class Log4jUtil {
  public static void loadProperties(Class<?> targetClass, String log4jPath) {
    InputStream is = null;
    try {
      is = targetClass.getResourceAsStream(log4jPath);
      Properties originalProperties = new Properties();
      originalProperties.load(is);
      LogManager.resetConfiguration();
      PropertyConfigurator.configure(originalProperties);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      IOUtils.closeQuietly(is);
    }
  }
}
