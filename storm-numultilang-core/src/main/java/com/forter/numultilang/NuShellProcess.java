package com.forter.numultilang;

import backtype.storm.task.TopologyContext;
import com.forter.numultilang.listeners.ShellMsgListener;
import com.forter.numultilang.listeners.ShellProcessErrorListener;
import com.forter.numultilang.messages.ShellBoltMsg;
import com.forter.numultilang.messages.ShellSetupMsg;
import com.forter.numultilang.listeners.ShellProcessStdErrListener;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.google.common.base.Throwables;
import com.zaxxer.nuprocess.NuProcess;
import com.zaxxer.nuprocess.NuProcessBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;

public class NuShellProcess implements Serializable, ShellMsgListener, ShellProcessErrorListener, ShellProcessStdErrListener {
    public static Logger LOG = LoggerFactory.getLogger(NuShellProcess.class);

    private NuProcess _subprocess;
    private String[]     command;
    public Number pid;
    public String componentName;
    private final ShellMsgListener messageListener;
    private final ShellProcessErrorListener errorListener;
    private NuShellProcessHandler handler;
    private CountDownLatch setupCountdownLatch;
    private boolean setupComplete;
    private ReentrantLock collectorLock = new ReentrantLock();
    private long collectorLockTimeoutMS;
    private StringBuffer stdErrorLog;
    private long stderrLogMaxLength;
    public Logger shellLogger;
    private Integer exitCode;

    public NuShellProcess(String[] command,
                          ShellMsgListener messageListener,
                          ShellProcessErrorListener errorListener) {
        this.command = command;
        this.messageListener = messageListener;
        this.errorListener = errorListener;
    }


    private ShellSerializer getSerializer(Map conf) {
        //get factory class name
        String serializer_className = String.valueOf(conf.get(NuShellConfig.TOPOLOGY_NUMULTILANG_SERIALIZER));
        LOG.info("Storm multilang serializer: " + serializer_className);

        ShellSerializer serializer = null;
        try {
            //create a factory class
            Class klass = Class.forName(serializer_className);
            //obtain a serializer object
            Object obj = klass.newInstance();
            serializer = (ShellSerializer)obj;
        } catch(Exception e) {
            throw new RuntimeException("Failed to construct multilang serializer from serializer " + serializer_className, e);
        }
        return serializer;
    }

    public Number launch(Map conf, TopologyContext context) {
        this.stdErrorLog = new StringBuffer();
        shellLogger = LoggerFactory.getLogger(context.getThisComponentId());
        this.collectorLockTimeoutMS = Long.valueOf(String.valueOf(conf.get(NuShellConfig.TOPOLOGY_NUMULTILANG_EMIT_TIMEOUT_MS)));
        long setupTimeoutMS = Long.valueOf(String.valueOf(conf.get(NuShellConfig.TOPOLOGY_NUMULTILANG_SETUP_TIMEOUT_MS)));
        this.setupCountdownLatch = new CountDownLatch(1);
        this.stderrLogMaxLength = Long.valueOf(String.valueOf(conf.get(NuShellConfig.TOPOLOGY_NUMULTILANG_STDERR_MAX_LENGTH)));
        NuProcessBuilder builder = new NuProcessBuilder(command);

        //Setting the current directory is not supported by NuProcessBulder
        //This would require the child process to manually change the current working directory
        //builder.directory(new File(context.getCodeDir()));

        this.componentName = context.getThisComponentId();
        this.handler = new NuShellProcessHandler(getSerializer(conf), this, this, this);
        builder.setProcessListener(handler);
        _subprocess = builder.start();

        ShellSetupMsg setupMsg = new ShellSetupMsg();
        setupMsg.setPidDir(context.getPIDDir());
        setupMsg.setConf(conf);
        setupMsg.setContext(context);
        handler.writeMessage(setupMsg);
        try {
            boolean ok = setupCountdownLatch.await(setupTimeoutMS, TimeUnit.MILLISECONDS);
            if (!ok) {
                throw Throwables.propagate(new TimeoutException("shell bolt " +componentName + " setup timed out."));
            }
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }

        return this.pid;
    }

    public void destroy(boolean force) {
        _subprocess.destroy(force);
    }

    /**
     * thread safe.
     */
    public void writeBoltMsg(ShellBoltMsg msg) {
        handler.writeMessage(msg);
    }

    public void writeTaskIds(List<Integer> taskIds) {
        handler.writeMessage(taskIds);
    }

    /**
     *
     * @return pid, if the process has been launched, null otherwise.
     */
    public Number getPid() {
        return this.pid;
    }

    /**
     *
     * @return the name of component.
     */
    public String getComponentName() {
        return this.componentName;
    }

    public String getProcessInfoString() {
        return String.format("pid:%s, name:%s", pid, componentName);
    }

    public String getProcessTerminationInfoString() {
        return String.format(" exitCode:%s, errorString:%s ", exitCode, stdErrorLog);
    }

    @Override
    public void onMessage(ShellResponseMsg msg) {
        lock();
        try {
            if (!setupComplete) {
                this.pid = msg.getPid();
                setupComplete = true;
                setupCountdownLatch.countDown();
                return;
            }
            messageListener.onMessage(msg);
        }
        finally {
            unlock();
        }
    }

    @Override
    public void onShellProcessStdError(String message) {
        shellLogger.info(message);
        if (stdErrorLog.length() < stderrLogMaxLength) {
            stdErrorLog.append(message);
        }
    }

    @Override
    public void onShellProcessExit(int exitCode) {
        this.exitCode = exitCode;
        lock();
        try {
            errorListener.onShellProcessExit(exitCode);
        }
        finally {
            unlock();
        }
    }

    @Override
    public void onInternalError(Throwable t) {
        lock();
        try {
            errorListener.onInternalError(t);
        }
        finally {
            unlock();
        }
    }

    private void unlock() {
        collectorLock.unlock();
    }

    private void lock() {
        try {
            if (!collectorLock.tryLock(collectorLockTimeoutMS, TimeUnit.MILLISECONDS)) {
                throw Throwables.propagate(new TimeoutException("Failed to acquire collector lock."));
            }
            ;
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        }
    }
}
