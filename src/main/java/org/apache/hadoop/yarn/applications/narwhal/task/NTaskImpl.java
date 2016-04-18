package org.apache.hadoop.yarn.applications.narwhal.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

  private LinkedHashMap<WorkerId, Worker> workers = new LinkedHashMap<>();
  private final EventHandler eventHandler;

  private final StateMachine<TaskState, TaskEventType, TaskEvent> stateMachine;

  //not sure if this is useful
  private static final ErrorTransition ERROR_TRANSITION =
      new ErrorTransition();

  private static final KillTransition KILL_TRANSITION =
      new KillTransition();

  public NTaskImpl(JobId jobId, int id, EventHandler eventHandler) {
    this.eventHandler = eventHandler;
    this.taskId = new TaskId(jobId, id);
    this.stateMachine = stateMachineFactory.make(this);
    ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    this.readLock = readWriteLock.readLock();
    this.writeLock = readWriteLock.writeLock();
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
      nTask.eventHandler.handle(new ContainerAllocatorEvent(nTask.getID(),
          ContainerAllocatorEventType.CONTAINERALLOCATOR_REQEUST));
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
      //if need to load image file , new a worker and post event to run it
      //when worker succeed in loading image, post a TASK_LAUNCH event
      boolean local = true;
      if (local) {
        if (currentWorkerSucceed(nTask)) {
          nTask.workers.clear();
          nTask.eventHandler.handle(new TaskEvent(taskEvent.getTaskID(),
              TaskEventType.TASK_LAUNCH));
          return TaskState.READY;
        }
        LOG.info("new Worker");
        String hostname = taskEvent.getContainer().getNodeId().getHost();
        NWorkerImpl worker = new NWorkerImpl(taskEvent.getTaskID(), 0,
            nTask.eventHandler, hostname);
        nTask.eventHandler.handle(new WorkerEvent(worker.getID(),
            WorkerEventType.WORKER_SCHEDULE));
        nTask.addWorker(worker);
        //state unchanged, wait for worker succeed here
        return TaskState.SCHEDULED;
      } else {
        nTask.eventHandler.handle(new TaskEvent(taskEvent.getTaskID(),
            TaskEventType.TASK_LAUNCH));
        return TaskState.READY;
      }
    }
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
      //post event to containerLauncher to launch the container
      nTask.eventHandler.handle(new ContainerLauncherEvent(taskEvent.getTaskID(),
          taskEvent.getContainer(),
          ContainerLauncherEventType.CONATAINERLAUNCHER_LAUNCH));
    }
  }

  private static class CompleteTransition implements
      MultipleArcTransition<NTaskImpl, TaskEvent, TaskState> {

    @Override
    public TaskState transition(NTaskImpl nTask, TaskEvent taskEvent) {
      //post an JOB_COMPLETE event
      nTask.eventHandler.handle(new JobEvent(taskEvent.getTaskID().getJobId(), JobEventType.JOB_COMPLETED));
      return TaskState.SUCCEED;
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

}
