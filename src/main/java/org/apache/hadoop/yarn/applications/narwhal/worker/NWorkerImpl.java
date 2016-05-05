package org.apache.hadoop.yarn.applications.narwhal.worker;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.applications.narwhal.event.*;
import org.apache.hadoop.yarn.applications.narwhal.state.WorkerState;
import org.apache.hadoop.yarn.applications.narwhal.task.TaskId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.MultipleArcTransition;
import org.apache.hadoop.yarn.state.SingleArcTransition;
import org.apache.hadoop.yarn.state.StateMachine;
import org.apache.hadoop.yarn.state.StateMachineFactory;

import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * launched by Task to do preparation. for instance, docker load image file
 */
public class NWorkerImpl implements Worker, EventHandler<WorkerEvent> {
  private static final Log LOG = LogFactory.getLog(NWorkerImpl.class);

  //this worker should work at this host. So it needs resource allocated from here to get things done
  private String hostname;

  private String cmd;

  //for instance, a worker to load image from this resource
  private String resourceName;

  private String resourcePath;

  private WorkerId workerId;

  private Lock writeLock;
  private final EventHandler eventHandler;
  private final StateMachine<WorkerState, WorkerEventType, WorkerEvent> stateMachine;

  public String getCmd() {
    return cmd;
  }

  private static class ScheduleTransition implements
      SingleArcTransition<NWorkerImpl, WorkerEvent> {

    @Override
    public void transition(NWorkerImpl nWorker, WorkerEvent workerEvent) {
      LOG.info("**WorkerScheduleTransition**");
      ContainerAllocatorEvent caEvent = new ContainerAllocatorEvent(nWorker.getID(),
          ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST);
      caEvent.setHostname(nWorker.getHostname());
      //TODO: this is hardcoded resource for worker
      Resource capability = Resource.newInstance(64,1);
      caEvent.setCapability(capability);
      caEvent.setPriority(0);
      nWorker.eventHandler.handle(caEvent);
    }
  }

  private static class KillTransition implements
      SingleArcTransition<NWorkerImpl, WorkerEvent> {

    @Override
    public void transition(NWorkerImpl nWorker, WorkerEvent workerEvent) {
      //killed, should release allocated contaienr or stop running container
      LOG.info("**WorkerKillTransition**");
    }
  }

  private static class StartTransition implements
      SingleArcTransition<NWorkerImpl, WorkerEvent> {

    @Override
    public void transition(NWorkerImpl nWorker, WorkerEvent workerEvent) {
      LOG.info("**WorkerStartTransition**");
      ContainerLauncherEvent containerLauncherEvent = new ContainerLauncherEvent(workerEvent.getWorkerId(),
          workerEvent.getContainer(),
          ContainerLauncherEventType.CONATAINERLAUNCHER_LAUNCH);
      containerLauncherEvent.setUserCmd(nWorker.getCmd());
      containerLauncherEvent.setResourceFileName(nWorker.getResourceName());
      containerLauncherEvent.setResourceFilePath(nWorker.getResourcePath());
      nWorker.eventHandler.handle(containerLauncherEvent);
    }
  }

  private static class CompleteTransition implements
      MultipleArcTransition<NWorkerImpl, WorkerEvent, WorkerState> {

    @Override
    public WorkerState transition(NWorkerImpl nWorker, WorkerEvent workerEvent) {
      LOG.info("**WorkerCompleteTransition**");
      ContainerStatus containerStatus = workerEvent.getContainerStatus();
      int returnValue = containerStatus.getExitStatus();
      if (returnValue == 0) {
        nWorker.eventHandler.handle(new TaskEvent(workerEvent.getWorkerId().getTaskId(),
            TaskEventType.TASK_SETUP));
        return WorkerState.SUCCEED;
      } else {
        LOG.info(nWorker.getID() + " failed with return value: " + returnValue);
        return WorkerState.FAILED;
      }
    }
  }

  protected static final StateMachineFactory<NWorkerImpl, WorkerState, WorkerEventType, WorkerEvent>
      stateMachineFactory
      = new StateMachineFactory<NWorkerImpl, WorkerState, WorkerEventType, WorkerEvent>(WorkerState.NEW)
      //Transitions from NEW
      //NEW -> SCHEDULED: ScheduleTransition
      .addTransition(WorkerState.NEW,
          WorkerState.SCHEDULED,
          WorkerEventType.WORKER_SCHEDULE,
          new ScheduleTransition())
      //NEW -> KILLED: KillTransition
      .addTransition(WorkerState.NEW,
          WorkerState.KILLED,
          WorkerEventType.WORKER_KILL,
          new KillTransition())
      //Transitions from SCHEDULED
      //SCHEDULED -> RUNNING: startTransition
      .addTransition(WorkerState.SCHEDULED,
          WorkerState.RUNNING,
          WorkerEventType.WORKER_START,
          new StartTransition())
      //Transitions from RUNNING
      //RUNNING -> (SUCCEED,FAILED): CompleteTransition
      .addTransition(WorkerState.RUNNING,
          EnumSet.of(WorkerState.SUCCEED,WorkerState.FAILED),
          WorkerEventType.WORKER_COMPLETED,
          new CompleteTransition())
      .installTopology();

  public NWorkerImpl(TaskId taskId, int id, EventHandler eventHandler,
                     String hostname, String cmd, String resourceName, String resourcePath) {
    this.hostname = hostname;
    this.workerId = new WorkerId(taskId,id);
    this.eventHandler = eventHandler;
    this.stateMachine = stateMachineFactory.make(this);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.writeLock = readWriteLock.writeLock();
    this.cmd = cmd;
    this.resourceName = resourceName;
    this.resourcePath = resourcePath;
  }

  @Override
  public void handle(WorkerEvent workerEvent) {
    LOG.info("Processing " + workerEvent.getWorkerId() + " of type " + workerEvent.getType());
    try {
      writeLock.lock();
      WorkerState oldState = getStatus();
      getStateMachine().doTransition(workerEvent.getType(), workerEvent);
      if (oldState != getStatus()) {
        LOG.info("task_" + workerId.getTaskId().getId()+ "_worker_" + workerId.getId() + " Transitioned from " + oldState + " to "
            + getStatus());
      }
    } catch (Exception e) {
      LOG.error("Can't handle this event at current state", e);
      eventHandler.handle(new JobEvent(this.workerId.getTaskId().getJobId(),
          JobEventType.JOB_ERROR));
    }
    finally {
      writeLock.unlock();
    }
  }

  @Override
  public WorkerId getID() {
    return workerId;
  }

  @Override
  public WorkerState getStatus() {
    return getStateMachine().getCurrentState();
  }

  public String getHostname() {
    return hostname;
  }

  public StateMachine<WorkerState, WorkerEventType, WorkerEvent> getStateMachine() {
    return stateMachine;
  }
  public String getResourceName() {
    return resourceName;
  }

  public void setResourceName(String resourceName) {
    this.resourceName = resourceName;
  }

  public String getResourcePath() {
    return resourcePath;
  }

  public void setResourcePath(String resourcePath) {
    this.resourcePath = resourcePath;
  }
}
