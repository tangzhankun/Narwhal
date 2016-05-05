package org.apache.hadoop.yarn.applications.narwhal.registry;

import com.google.common.base.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.io.IOException;

public class NRegistryOperator {

  private RegistryOperations registryOperations;

  private final String user;
  private Configuration conf;
  private final String narwhalServiceClass;
  private final String instanceName;

  private ServiceRecord selfRegistration;

  private String selfRegistrationPath;

  public NRegistryOperator(String user, String narwhalServiceClass, String instanceName, Configuration conf) {
    this.conf = conf;
    Preconditions.checkArgument(user != null, "null user");
    Preconditions.checkArgument(isSet(narwhalServiceClass), "unset service class");
    Preconditions.checkArgument(isSet(instanceName), "instanceName");
    this.user = user;
    this.narwhalServiceClass = narwhalServiceClass;
    this.instanceName = instanceName;

    createRegistryOperationsInstance();

  }

  public String getUser() {
    return user;
  }

  public String getNarwhalServiceClass() {
    return narwhalServiceClass;
  }

  public String getInstanceName() {
    return instanceName;
  }

  public RegistryOperations getRegistryOperations() {
    return registryOperations;
  }

  public ServiceRecord getSelfRegistration() {
    return selfRegistration;
  }

  private void setSelfRegistration(ServiceRecord selfRegistration) {
    this.selfRegistration = selfRegistration;
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

  public String putService(String username, String serviceClass, String serviceName, ServiceRecord record, boolean deleteTreeFirst) throws IOException {
    String path = RegistryUtils.servicePath(username, serviceClass, serviceName);
    if (deleteTreeFirst) {
      registryOperations.delete(path, true);
    }
    registryOperations.mknode(RegistryPathUtils.parentOf(path), true);
    registryOperations.bind(path, record, BindFlags.OVERWRITE);
    return path;
  }

  public String registerSelf(ServiceRecord record, boolean deleteTreeFirst) throws IOException {
    selfRegistrationPath = putService(user, narwhalServiceClass, instanceName, record, deleteTreeFirst);
    setSelfRegistration(record);
    return selfRegistrationPath;
  }

  public void updateSelf() throws IOException {
    putService(user, narwhalServiceClass, instanceName, selfRegistration, false);
  }

  public ServiceRecord resolve() throws NoRecordException, InvalidRecordException, PathNotFoundException, IOException {
    String path = RegistryUtils.servicePath(user, narwhalServiceClass, instanceName);
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

  public static boolean isUnset(String s) {
    return s == null || s.isEmpty();
  }

  public static boolean isSet(String s) {
    return !isUnset(s);
  }

}
