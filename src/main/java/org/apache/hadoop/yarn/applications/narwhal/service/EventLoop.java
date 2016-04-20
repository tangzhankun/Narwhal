package org.apache.hadoop.yarn.applications.narwhal.service;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.applications.narwhal.NAppMaster;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEvent;
import org.apache.hadoop.yarn.applications.narwhal.event.JobEventType;
import org.apache.hadoop.yarn.applications.narwhal.job.Job;
import org.apache.hadoop.yarn.event.AbstractEvent;
import org.apache.hadoop.yarn.event.EventHandler;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * loop the events to handle it
 */
public abstract class EventLoop {

  private static final Log LOG = LogFactory.getLog(EventLoop.class);

  private Thread eventHandlingThread;
  private final AtomicBoolean stopped;
  protected NAppMaster.AppContext context;
  protected EventHandler eventHandler;
  protected Job job;
  protected BlockingQueue<AbstractEvent> eventQueue
      = new LinkedBlockingDeque<AbstractEvent>();

  public EventLoop(NAppMaster.AppContext context) {
    this.stopped = new AtomicBoolean(false);
    this.eventHandler = context.getEventHandler();
    this.job = context.getJob();
    this.context = context;
  }

  public void start() throws Exception {
    this.eventHandlingThread = new Thread() {
      public void run() {
        AbstractEvent event = null;
        while(!stopped.get() && !Thread.currentThread().isInterrupted()) {
          try {
            event = eventQueue.take();
          } catch (InterruptedException e) {
            if (!stopped.get()) {
              e.printStackTrace();
            }
          }
          try {
            processEvent(event);
          } catch (Throwable t) {
            eventHandler.handle(new JobEvent(job.getID(), JobEventType.JOB_ERROR));
          }
        }
      }//run()
    };//thread
    LOG.info("service event loop started");
    this.eventHandlingThread.start();
    startClientAsync();
  }

  public abstract void stop();
  public abstract void processEvent(AbstractEvent event);
  public abstract void startClientAsync();
}
