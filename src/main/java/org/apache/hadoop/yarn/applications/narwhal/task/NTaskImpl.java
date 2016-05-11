package org.apache.hadoop.yarn.applications.narwhal.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.narwhal.common.DateUtil;
import org.apache.hadoop.yarn.applications.narwhal.common.ImageUtil;
import org.apache.hadoop.yarn.applications.narwhal.common.NRegistryOperator;
import org.apache.hadoop.yarn.applications.narwhal.common.NarwhalConstant;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.state.TaskState;
import org.apache.hadoop.yarn.applications.narwhal.job.JobId;
import org.apache.hadoop.yarn.applications.narwhal.state.WorkerState;
import org.apache.hadoop.yarn.applications.narwhal.worker.NWorkerImpl;
import org.apache.hadoop.yarn.applications.narwhal.worker.Worker;
import org.apache.hadoop.yarn.applications.narwhal.worker.WorkerId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class NTaskImpl implements Task, EventHandler<TaskEvent>{

  private static final Log LOG = LogFactory.getLog(NTaskImpl.class);

  private TaskId taskId;
  private String hostname;
  private String rackname;
  private String port;
  private Lock readLock;
  private Lock writeLock;

  private Container container;

  //task config info
  private String appName;
  private String appId;
  private String userCmd;
  private int mem;
  private int cpu;
  private int pri;
  private String imageName;
  private boolean isUsingLocalImage;

  private LinkedHashMap<WorkerId, Worker> workers = new LinkedHashMap<>();
  private final EventHandler eventHandler;
  private NRegistryOperator nRegistryOperator;

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;

  public String getUserCmd() {
    return userCmd;
  }

  public String getImageName() {
    return imageName;
  }

  public void setContainer(Container container) {
    LOG.info(getID() + " got container:" + container.getId());
    this.container = container;
  }

  public Container getContainer() {
    return container;
  }
  //not sure if this is useful
  private static final ErrorTransition ERROR_TRANSITION =
      new ErrorTransition();

  private static final KillTransition KILL_TRANSITION =
      new KillTransition();

  public NTaskImpl(JobId jobId, int id, EventHandler eventHandler,
                   String userCmd, int cpu, int mem, int pri,
                   String imageName, boolean useLocalImage, String appName, NRegistryOperator nRegistryOperator) {
    this.eventHandler = eventHandler;
    this.taskId = new TaskId(jobId, id);
    this.stateMachine = stateMachineFactory.make(this);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
    this.userCmd = userCmd;
    this.mem = mem;
    this.cpu = cpu;
    this.pri = pri;
    this.isUsingLocalImage = useLocalImage;
    this.imageName = imageName;
    this.appName = appName;
    this.nRegistryOperator = nRegistryOperator;
  }

  private static class ErrorTransition implements
      SingleArcTransition<NTaskImpl, TaskEvent> {

    @Override
    public void transition(NTaskImpl nTask, TaskEvent taskEvent) {
      //handle error and post Job complete event
    }
  }

  private static class ScheduleTransition implements
      SingleArcTransition<NTaskImpl, TaskEvent> {

    @Override
    public void transition(NTaskImpl nTask, TaskEvent taskEvent) {
      LOG.info("**TaskScheduleTransition**");
      //post an event to ContainerAllocator for allocate container
      ContainerAllocatorEvent containerAllocatorEvent = new ContainerAllocatorEvent(nTask.getID(),
          ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
      Resource capability = Resource.newInstance(nTask.getMem(),nTask.getCpu());
      containerAllocatorEvent.setCapability(capability);
      containerAllocatorEvent.setPriority(nTask.getPri());
      containerAllocatorEvent.setHostname("");
      nTask.eventHandler.handle(containerAllocatorEvent);
    }

  }

  private static class KillTransition implements
      SingleArcTransition<NTaskImpl, TaskEvent> {

    @Override
    public void transition(NTaskImpl nTask, TaskEvent taskEvent) {
      //killed, should release allocated contaienr or stop running container
    }
  }

  private static class SetupTransition implements
      MultipleArcTransition<NTaskImpl, TaskEvent, TaskState> {

    @Override
    public TaskState transition(NTaskImpl nTask, TaskEvent taskEvent) {
      LOG.info("**TaskSetupTransition**");
      //task should save this container if not null
      //because the taskEvent without container is posted from worker
      if (taskEvent.getContainer() != null) {
        nTask.setContainer(taskEvent.getContainer());
      }
      //if need to load image file , new a worker and post event to run it
      //when worker succeed in loading image, post a TASK_LAUNCH event
      if (nTask.isUsingLocalImage) {
        if (currentWorkerSucceed(nTask)) {
          LOG.info("All " + nTask.getID() + " 's worker finished");
          nTask.workers.clear();
          postTaskLaunchEvent(nTask, taskEvent.getTaskID());
          return TaskState.READY;
        }
        LOG.info("** new Worker **");
        String hostname = taskEvent.getContainer().getNodeId().getHost();
        //TODO: this should be docker load image file
        String resourceName = ImageUtil.getFSFileName(nTask.getImageName());
        String resourcePath = ImageUtil.getFSFilePathSuffix(nTask.getAppName(), nTask.getAppId(), resourceName);
        String workerCmd = "docker load -i ./" + resourceName;
        NWorkerImpl worker = new NWorkerImpl(taskEvent.getTaskID(), 0,
            nTask.eventHandler, hostname, workerCmd, resourceName, resourcePath);
        nTask.eventHandler.handle(new WorkerEvent(worker.getID(),
            WorkerEventType.WORKER_SCHEDULE));
        nTask.addWorker(worker);
        //state unchanged, wait for worker succeed here
        return TaskState.SCHEDULED;
      } else {
        postTaskLaunchEvent(nTask, taskEvent.getTaskID());
        return TaskState.READY;
      }
    }
    private void postTaskLaunchEvent(NTaskImpl nTask, TaskId taskid) {
      TaskEvent event = new TaskEvent(taskid,
          TaskEventType.TASK_LAUNCH);
      event.setContainer(nTask.getContainer());
      setContainerScheduledRecord(nTask);
      nTask.eventHandler.handle(event);
    }
    private void setContainerScheduledRecord(NTaskImpl nTask) {
      Container allocatedContainer = nTask.container;
      String containerIdStr = allocatedContainer.getId().toString();
      nTask.nRegistryOperator.setContainerRecord(containerIdStr, NarwhalConstant.HOST, allocatedContainer.getNodeId().getHost());
      nTask.nRegistryOperator.setContainerRecord(containerIdStr, NarwhalConstant.PORT, String.valueOf(allocatedContainer.getNodeId().getPort()));
      nTask.nRegistryOperator.setContainerRecord(containerIdStr, NarwhalConstant.CREATED, DateUtil.getCurrentTime());
      nTask.nRegistryOperator.setContainerRecord(containerIdStr, NarwhalConstant.STATUS, TaskState.SCHEDULED.toString());
      nTask.nRegistryOperator.updateContainer(containerIdStr);
    }

    //TODO: zhankun, use counter to check this to avoid multiple worker issue
    public boolean currentWorkerSucceed(NTaskImpl nTask) {
      if (nTask.workers.size() == 0) {
        return false;
      }
      int finishedNum = 0;
      for (Worker worker : nTask.workers.values()) {
        if (worker.getStatus().equals(WorkerState.SUCCEED)) {
          finishedNum ++;
        }
      }
      return finishedNum == nTask.workers.size();
    }
  }

  private static class LaunchTransition implements
      SingleArcTransition<NTaskImpl, TaskEvent> {

    @Override
    public void transition(NTaskImpl nTask, TaskEvent taskEvent) {
      LOG.info("**TaskLaunchTransition**");
      setContainerRunningRecord(nTask);
      //post event to containerLauncher to launch the container
      ContainerLauncherEvent containerLauncherEvent = new ContainerLauncherEvent(taskEvent.getTaskID(),
          taskEvent.getContainer(),
          ContainerLauncherEventType.CONATAINERLAUNCHER_LAUNCH);
      containerLauncherEvent.setUserCmd(nTask.getUserCmd());
      containerLauncherEvent.setDockerImageName(nTask.getImageName());
      nTask.eventHandler.handle(containerLauncherEvent);
    }

    private void setContainerRunningRecord(NTaskImpl nTask) {
      nTask.nRegistryOperator.setContainerRecord(nTask.container.getId().toString(), NarwhalConstant.COMMAND, nTask.userCmd);
      nTask.nRegistryOperator.setContainerRecord(nTask.container.getId().toString(), NarwhalConstant.IMAGE, nTask.imageName);
      nTask.nRegistryOperator.setContainerRecord(nTask.container.getId().toString(), NarwhalConstant.STATUS, TaskState.RUNNING.toString());
      nTask.nRegistryOperator.updateContainer(nTask.container.getId().toString());
    }
  }

  private static class CompleteTransition implements
      MultipleArcTransition<NTaskImpl, TaskEvent, TaskState> {

    @Override
    public TaskState transition(NTaskImpl nTask, TaskEvent taskEvent) {
      //post an JOB_COMPLETE event
      setContainerSucceedRecord(nTask);
      nTask.eventHandler.handle(new JobEvent(taskEvent.getTaskID().getJobId(), JobEventType.JOB_COMPLETED));
      return TaskState.SUCCEED;
    }
    
    private void setContainerSucceedRecord(NTaskImpl nTask) {
      Container container = nTask.getContainer();
      String startedContainerId  = container.getId().toString();
      nTask.nRegistryOperator.setContainerRecord(startedContainerId.toString(), NarwhalConstant.STATUS, TaskState.SUCCEED.toString());
      nTask.nRegistryOperator.updateContainer(startedContainerId.toString());
    }
  }

  protected static final StateMachineFactory<NTaskImpl, TaskState, TaskEventType, TaskEvent>
      stateMachineFactory
      = new StateMachineFactory<NTaskImpl, TaskState, TaskEventType, TaskEvent>(TaskState.NEW)
      //Transitions from NEW
      //NEW -> SCHEDULED: ScheduleTransition
      .addTransition(TaskState.NEW,
          TaskState.SCHEDULED,
          TaskEventType.TASK_SCHEDULE,
          new ScheduleTransition())
      //NEW -> KILLED: KillTransition
      .addTransition(TaskState.NEW,
          TaskState.KILLED,
          TaskEventType.TASK_KILL,
          KILL_TRANSITION)
      //Transitions from SCHEDULED
      //SCHEDULED -> (SCHEDULED,READY,FAILED): SetupTransition
      .addTransition(TaskState.SCHEDULED,
          EnumSet.of(TaskState.SCHEDULED, TaskState.READY, TaskState.FAILED),
          TaskEventType.TASK_SETUP,
          new SetupTransition())
      //Transitions from READY: LaunchTransition
      //READY -> RUNNING: LaunchTransition
      .addTransition(TaskState.READY,
          TaskState.RUNNING,
          TaskEventType.TASK_LAUNCH,
          new LaunchTransition())
      //Transitions from RUNNING
      //RUNNING -> (SUCCEED,FAILED): CompleteTransition
      .addTransition(TaskState.RUNNING,
          EnumSet.of(TaskState.SUCCEED, TaskState.FAILED),
          TaskEventType.TASK_COMPLETED,
          new CompleteTransition())
      //RUNNING -> KILLED: KillTransition
      .addTransition(TaskState.RUNNING,
          TaskState.KILLED,
          TaskEventType.TASK_KILL,
          KILL_TRANSITION)
      .installTopology();

  @Override
  public void handle(TaskEvent taskEvent) {
    LOG.info("Processing " + taskEvent.getTaskID() + " of type " + taskEvent.getType());
    try {
      writeLock.lock();
      TaskState oldState = getStatus();
      getStateMachine().doTransition(taskEvent.getType(), taskEvent);
      if (oldState != getStatus()) {
        LOG.info("task_" + taskId.getId() + " Transitioned from " + oldState + " to "
            + getStatus());
      }
    } catch (Exception e) {
      LOG.error("Can't handle this event at current state", e);
      eventHandler.handle(new JobEvent(this.taskId.getJobId(), JobEventType.JOB_ERROR));
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public TaskId getID() {
    return taskId;
  }

  @Override
  public TaskState getStatus() {
    return getStateMachine().getCurrentState();
  }

  @Override
  public Worker getWorker(WorkerId workerId) {
    readLock.lock();
    try {
      return workers.get(workerId);
    } finally {
      readLock.unlock();
    }
  }

  public StateMachine<TaskState, TaskEventType, TaskEvent> getStateMachine() {
    return stateMachine;
  }

  protected void addWorker(Worker worker) {
    workers.put(worker.getID(), worker);
  }

  public int getMem() {
    return mem;
  }

  public int getCpu() {
    return cpu;
  }

  public int getPri() {
    return pri;
  }

  public boolean isUsingLocalImage() {
    return isUsingLocalImage;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public String getAppId() {
    return taskId.getJobId().getAppId().toString();
  }
}
