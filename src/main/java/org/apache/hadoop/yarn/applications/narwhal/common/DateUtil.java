package org.apache.hadoop.yarn.applications.narwhal.common;

import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;

public class DateUtil {
  public static String getCurrentTime() {
    return DateFormatUtils.format(new Date(), "yyyy-MM-dd HH:mm:ss");
  }
}
