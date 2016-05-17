package org.apache.hadoop.yarn.applications.narwhal.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NRegistryOperator {

  private static final Log LOG = LogFactory.getLog(NRegistryOperator.class);

  private static RegistryOperations registryOperations;

  private String user;
  private String appIdAsName;
  private String serviceClass = "org-apache-narwhal";

  private ServiceRecord appRecord;
  private Map<String, ServiceRecord> containerRecords;
  private Configuration conf;

  public NRegistryOperator(String appIdAsName, Configuration conf) {
    this.conf = conf;
    this.appIdAsName = appIdAsName;
    this.user = RegistryUtils.currentUser();
    createRegistryOperationsInstance();
    initServiceRecord();
  }

  public NRegistryOperator(Configuration conf) {
    this.conf = conf;
    this.user = RegistryUtils.currentUser();
    createRegistryOperationsInstance();
  }

  private void putService() throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, appRecord, BindFlags.OVERWRITE);
  }

  private void putComponent(String containerIdAsName) throws IOException {
    String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerIdAsName);
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, containerRecords.get(containerIdAsName), BindFlags.OVERWRITE);
  }

  public void updateApp() {
    try {
      putService();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void deleteApp() {
    try {
      String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
      boolean exists = registryOperations.exists(path);
      if (exists) {
        registryOperations.delete(path, true);
      } else {
        LOG.info(path + "cannot be found");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public ServiceRecord resolveApp() throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    boolean exists = registryOperations.exists(path);
    if (exists) {
      ServiceRecord record = registryOperations.resolve(path);
      return record;
    } else {
      LOG.info(path + "cannot be found");
      return null;
    }
  }

  public List<String> listNarwhalApps() throws IOException {
    String path = RegistryUtils.serviceclassPath(user, serviceClass);
    boolean exists = registryOperations.exists(path);
    if (exists) {
      List<String> appIds = registryOperations.list(path);
      return appIds;
    } else {
      LOG.info(path + "cannot be found");
      return new ArrayList<String>();
    }
  }

  public void updateContainer(String containerIdAsName) {
    try {
      putComponent(containerIdAsName);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void deleteContainer(String containerIdAsName) {
    try {
      String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerIdAsName);
      boolean exists = registryOperations.exists(path);
      if (exists) {
        registryOperations.delete(path, false);
      } else {
        LOG.info(path + "cannot be found");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public ServiceRecord resolveContainer(String containerIdAsName) throws IOException {
    String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerIdAsName);
    boolean exists = registryOperations.exists(path);
    if (exists) {
      ServiceRecord record = registryOperations.resolve(path);
      return record;
    } else {
      LOG.info(path + "cannot be found");
      return null;
    }
  }

  public Map<String, ServiceRecord> resolveContainers() throws IOException {
    Map<String, ServiceRecord> records = new HashMap<String, ServiceRecord>();

    List<String> containerIds = listAllContainers();
    if (containerIds.isEmpty()) {
      return null;
    }

    for (String containerId : containerIds) {
      String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerId);
      ServiceRecord record = registryOperations.resolve(path);
      records.put(containerId, record);
    }
    return records;
  }

  public List<String> listAllContainers() throws IOException {
    String path = RegistryUtils.componentListPath(user, serviceClass, appIdAsName);
    boolean exists = registryOperations.exists(path);
    if (exists) {
      List<String> containerIds = registryOperations.list(path);
      return containerIds;
    } else {
      LOG.info(path + "cannot be found");
      return new ArrayList<String>();
    }

  }

  private RegistryOperations createRegistryOperationsInstance() {
    if (registryOperations == null) {
      registryOperations = RegistryOperationsFactory.createInstance("YarnRegistry", conf);
      registryOperations.start();
    }
    return registryOperations;
  }

  public void destroyRegistryOperationsInstance() {
    if (registryOperations != null) {
      registryOperations.stop();
    }
  }

  private void initServiceRecord() {
    appRecord = new ServiceRecord();
    appRecord.description = "Narwhal Application Master";
    containerRecords = new HashMap<String, ServiceRecord>();
  }

  private ServiceRecord initContainerServiceRecord() {
    ServiceRecord containerRecord = new ServiceRecord();
    containerRecord.description = "Narwhal Container";
    return containerRecord;
  }

  public void setAppRecord(String key, String value) {
    appRecord.set(key, value);
  }

  public void setContainerRecord(String containerId, String key, String value) {
    ServiceRecord record = containerRecords.get(containerId);
    if (record == null) {
      record = initContainerServiceRecord();
    }
    record.set(key, value);
    containerRecords.put(containerId, record);
  }

}
