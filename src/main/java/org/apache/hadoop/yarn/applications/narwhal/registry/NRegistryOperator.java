package org.apache.hadoop.yarn.applications.narwhal.registry;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.io.IOException;

public class NRegistryOperator {

  private RegistryOperations registryOperations;

  private final String user;
  private final String appIdAsName;
  private final String serviceClass = "org-apache-narwhal";

  private ServiceRecord record;
  private String selfRegistrationPath;
  private Configuration conf;
  
  public NRegistryOperator(String appIdAsName, Configuration conf) {
    this.conf = conf;
    Preconditions.checkArgument(isSet(appIdAsName), "instanceName");
    this.appIdAsName = appIdAsName;
    this.user = RegistryUtils.currentUser();
    createRegistryOperationsInstance();
    initServiceRecord();
  }

  public String getUser() {
    return user;
  }

  public RegistryOperations getRegistryOperations() {
    return registryOperations;
  }

  public String getSelfRegistrationPath() {
    return selfRegistrationPath;
  }

  public String getAbsoluteSelfRegistrationPath() {
    if (selfRegistrationPath == null) {
      return null;
    }
    String root = registryOperations.getConfig().getTrimmed(RegistryConstants.KEY_REGISTRY_ZK_ROOT, RegistryConstants.DEFAULT_ZK_REGISTRY_ROOT);
    return RegistryPathUtils.join(root, selfRegistrationPath);
  }

  public String putService(String appIdAsName, boolean deleteTreeFirst) throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    if (deleteTreeFirst) {
      registryOperations.delete(path, true);
    }
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, record, BindFlags.OVERWRITE);
    return path;
  }

  public String register(boolean deleteTreeFirst) throws IOException {
    selfRegistrationPath = putService(appIdAsName, deleteTreeFirst);
    return selfRegistrationPath;
  }

  public void update() throws IOException {
    putService(appIdAsName, false);
  }
  
  public void delete() throws IOException {
  	registryOperations.delete(selfRegistrationPath, true);
  }
  
  public ServiceRecord resolve() throws IOException {
    String path = RegistryUtils.servicePath(user, serviceClass, appIdAsName);
    ServiceRecord record = registryOperations.resolve(path);
    return record;
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
  	record = new ServiceRecord();
  	record.description = "Narwhal Application Master";
  }
  
  public void set(String key, String value){
  	record.set(key, value);
  }

  public static boolean isUnset(String s) {
    return s == null || s.isEmpty();
  }

  public static boolean isSet(String s) {
    return !isUnset(s);
  }

}
