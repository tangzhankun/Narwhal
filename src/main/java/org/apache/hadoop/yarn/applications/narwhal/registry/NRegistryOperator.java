package org.apache.hadoop.yarn.applications.narwhal.registry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NRegistryOperator {

  private static RegistryOperations registryOperations;

  private String user;
  private String appIdAsName;
  private String serviceClass = "org-apache-narwhal";

  private ServiceRecord appRecord;
  private ServiceRecord containerRecord;
  private String appPath;
  private String containerPath;
  private Configuration conf;
  
  public NRegistryOperator(String appIdAsName, Configuration conf) {
    this.conf = conf;
    this.appIdAsName = appIdAsName;
    this.user = RegistryUtils.currentUser();
    createRegistryOperationsInstance();
    initServiceRecord();
  }

  private String putService() throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, appRecord, BindFlags.OVERWRITE);
    return path;
  }
  
  private String putComponent(String containerIdAsName) throws IOException {
    String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerIdAsName);
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, containerRecord, BindFlags.OVERWRITE);
    return path;
  }

  public String registerApp() throws IOException {
    appPath = putService();
    return appPath;
  }

  public void updateApp() throws IOException {
    putService();
  }
  
  public void deleteApp() throws IOException {
  	String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
  	registryOperations.delete(path, true);
  }
  
  public ServiceRecord resolveApp() throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    ServiceRecord record = registryOperations.resolve(path);
    return record;
  }
  
  public List<String> listNarwhalApps() throws IOException{
  	String path = RegistryUtils.serviceclassPath(user, serviceClass);
  	List<String> appIds = registryOperations.list(path);
  	return appIds;
  }
  
  public String registerContainer(String containerIdAsName) throws IOException {
  	containerPath = putComponent(containerIdAsName);
  	return containerPath;
  }
  
  public void updateContainer(String containerIdAsName) throws IOException{
  	putComponent(containerIdAsName);
  }
  
  public void deleteContainer(String containerIdAsName) throws IOException {
    String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerIdAsName);
    registryOperations.delete(path, false);
  }
  
  public Map<String, ServiceRecord> resolveContainers() throws IOException {
  	Map<String, ServiceRecord> records = new HashMap<String, ServiceRecord>();
  	
  	List<String> containerIds = listAllContainers();
  	for(String containerId : containerIds) {
  		String path = RegistryUtils.componentPath(user, serviceClass, appIdAsName, containerId);	
  		ServiceRecord record = registryOperations.resolve(path);
  		records.put(containerId, record);
  	}
    return records;
  }
  
  public List<String> listAllContainers() throws IOException{
  	String path = RegistryUtils.componentListPath(user, serviceClass, appIdAsName);
  	List<String> containerIds = registryOperations.list(path);
  	return containerIds;
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
  
  private void initServiceRecord(){
  	appRecord = new ServiceRecord();
  	appRecord.description = "Narwhal Application Master";
  	containerRecord = new ServiceRecord();
  	containerRecord.description = "Narwhal Container";
  }
  
  public void setAppRecord(String key, String value){
  	appRecord.set(key, value);
  }
  
  public void setContainerRecord(String key, String value) {
  	containerRecord.set(key, value);
  }
  
  public String getAppPath() {
  	return appPath;
  }
  
  public String getContainerPath() {
  	return containerPath;
  }
  
}
