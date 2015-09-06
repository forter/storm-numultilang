/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.forter.numultilang;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.generated.ShellComponent;
import backtype.storm.metric.api.IMetric;
import backtype.storm.metric.api.rpc.IShellMetric;
import backtype.storm.multilang.ShellMsg;
import backtype.storm.task.IBolt;
import backtype.storm.task.IOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.ReportedFailedException;
import backtype.storm.tuple.Tuple;
import clojure.lang.RT;
import com.forter.numultilang.listeners.ShellMsgListener;
import com.forter.numultilang.listeners.ShellProcessErrorListener;
import com.forter.numultilang.messages.ShellBoltMsg;
import com.forter.numultilang.exceptions.HeartbeatException;
import com.forter.numultilang.exceptions.ShellBoltException;
import com.forter.numultilang.exceptions.ShellProcessExitException;
import com.forter.numultilang.messages.ShellResponseMsg;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bolt that shells out to another process to process tuples. ShellBolt
 * communicates with that process over stdio using a special protocol. An ~100
 * line library is required to implement that protocol, and adapter libraries
 * currently exist for Ruby and Python.
 *
 * <p>To run a ShellBolt on a cluster, the scripts that are shelled out to must be
 * in the resources directory within the jar submitted to the master.
 * During development/testing on a local machine, that resources directory just
 * needs to be on the classpath.</p>
 *
 * <p>When creating topologies using the Java API, subclass this bolt and implement
 * the IRichBolt interface to create components for the topology that use other languages. For example:
 * </p>
 *
 * <pre>
 * public class MyBolt extends NuShellBolt implements IRichBolt {
 *      public MyBolt() {
 *          super("python", "mybolt.py");
 *      }
 *
 *      public void declareOutputFields(OutputFieldsDeclarer declarer) {
 *          declarer.declare(new Fields("field1", "field2"));
 *      }
 * }
 * </pre>
 *
 * This is a modification of ShellBolt uses NuProcess which does not require threads for handling stdin/stdout.
 * Setting the child process' current directory is not supported by NuProcessBulder
 * This would require the child process to manually change the current working directory
 */
public class NuShellBolt implements IBolt, ShellMsgListener, ShellProcessErrorListener {
    public static final String HEARTBEAT_STREAM_ID = "__heartbeat";
    public static final Logger LOG = LoggerFactory.getLogger(NuShellBolt.class);
    private static final Random random = new Random(System.currentTimeMillis());
    private static final ScheduledExecutorService heartBeatExecutorService =
            (ScheduledExecutorService) MoreExecutors.getExitingScheduledExecutorService(
                    new ScheduledThreadPoolExecutor(1, new HeartbeatThreadFactory()));

    IOutputCollector _collector;
    Map<String, Tuple> _inputs = new ConcurrentHashMap<String, Tuple>();

    private String[] _command;
    private NuShellProcess _process;
    private volatile boolean _running = true;
    private volatile Throwable _exception;
    private Random _rand;

    private TopologyContext _context;

    private int heartbeatTimeoutMills;
    private AtomicLong lastHeartbeatTimestamp = new AtomicLong();
    private Long pid;

    public NuShellBolt(ShellComponent component) {
        this(component.get_execution_command(), component.get_script());
    }

    public NuShellBolt(String... command) {
        _command = command;
    }

    @Override
    public void prepare(Map stormConf, final TopologyContext context,
                        final OutputCollector collector) {
        prepare(stormConf, context, (IOutputCollector)collector);
    }

    protected void prepare(Map stormConf, final TopologyContext context,
                        final IOutputCollector collector) {

        _rand = new Random();
        _collector = collector;

        _context = context;

        heartbeatTimeoutMills = getHeartbeatTimeoutMillis(stormConf);

        _process = new NuShellProcess(_command, this, this);

        //subprocesses must send their pid first thing
        Number subpid = _process.launch(stormConf, context);
        LOG.info("Launched subprocess with pid " + subpid);
        this.pid = subpid.longValue();



        /**
         * randomizing the initial delay would prevent all shell bolts from heartbeating at the same time frame
         */
        int initialDelayMillis = random.nextInt(4000) + 1000;
        BoltHeartbeatTimerTask task = new BoltHeartbeatTimerTask(this);
        heartBeatExecutorService.scheduleAtFixedRate(task, initialDelayMillis, getHeartbeatPeriodMillis(stormConf), TimeUnit.MILLISECONDS);
    }

    protected int getHeartbeatPeriodMillis(Map stormConf) {
        return (int)TimeUnit.SECONDS.toMillis(10);
    }

    protected int getHeartbeatTimeoutMillis(Map stormConf) {
        return (int)TimeUnit.SECONDS.toMillis(RT.intCast(stormConf.get(Config.SUPERVISOR_WORKER_TIMEOUT_SECS)));
    }

    public void execute(Tuple input) {
        if (_exception != null) {
            throw new RuntimeException(_exception);
        }

        //just need an id
        String genId = Long.toString(_rand.nextLong());
        _inputs.put(genId, input);
        _process.writeBoltMsg(createBoltMessage(input, genId));
    }

    private ShellBoltMsg createBoltMessage(Tuple input, String genId) {
        ShellBoltMsg boltMsg = new ShellBoltMsg();
        boltMsg.setId(genId);
        boltMsg.setComp(input.getSourceComponent());
        boltMsg.setStream(input.getSourceStreamId());
        boltMsg.setTask(input.getSourceTask());
        boltMsg.setTuple(input.getValues());
        return boltMsg;
    }

    public void cleanup() {
        _running = false;
        heartBeatExecutorService.shutdownNow();
        _process.destroy(/*force=*/true);
        _inputs.clear();
    }

    private void handleAck(Object id) {
        Tuple acked = _inputs.remove(id);
        if(acked==null) {
            throw new RuntimeException("Acked a non-existent or already acked/failed id: " + id);
        }
        _collector.ack(acked);
    }

    private void handleFail(Object id) {
        Tuple failed = _inputs.remove(id);
        if(failed==null) {
            throw new RuntimeException("Failed a non-existent or already acked/failed id: " + id);
        }
        _collector.fail(failed);
    }

    private void handleError(String msg) {
        _collector.reportError(new Exception("Shell Process Exception: " + msg));
    }

    private void handleEmit(ShellResponseMsg shellMsg) {
        List<Tuple> anchors = new ArrayList<Tuple>();
        List<String> recvAnchors = shellMsg.getAnchors();
        if (recvAnchors != null) {
            for (String anchor : recvAnchors) {
                Tuple t = _inputs.get(anchor);
                if (t == null) {
                    throw new RuntimeException("Anchored onto " + anchor + " after ack/fail");
                }
                anchors.add(t);
            }
        }

        if(shellMsg.getTask() == 0) {
            List<Integer> outtasks = _collector.emit(shellMsg.getStream(), anchors, shellMsg.getTuple());
            if (shellMsg.areTaskIdsNeeded()) {
                Preconditions.checkNotNull(outtasks);
                _process.writeTaskIds(outtasks);
            }
        } else {
            _collector.emitDirect((int) shellMsg.getTask(),
                    shellMsg.getStream(), anchors, shellMsg.getTuple());
        }
    }

    private void handleLog(ShellMsg shellMsg) {
        String msg = shellMsg.getMsg();
        msg = "ShellLog " + _process.getProcessInfoString() + " " + msg;
        ShellMsg.ShellLogLevel logLevel = shellMsg.getLogLevel();

        switch (logLevel) {
            case TRACE:
                LOG.trace(msg);
                break;
            case DEBUG:
                LOG.debug(msg);
                break;
            case INFO:
                LOG.info(msg);
                break;
            case WARN:
                LOG.warn(msg);
                break;
            case ERROR:
                LOG.error(msg);
                _collector.reportError(new ReportedFailedException(msg));
                break;
            default:
                LOG.info(msg);
                break;
        }
    }

    private void handleMetrics(ShellMsg shellMsg) {
        //get metric name
        String name = shellMsg.getMetricName();
        if (name.isEmpty()) {
            throw new RuntimeException("Receive Metrics name is empty");
        }

        //get metric by name
        IMetric iMetric = _context.getRegisteredMetricByName(name);
        if (iMetric == null) {
            throw new RuntimeException("Could not find metric by name["+name+"] ");
        }
        if ( !(iMetric instanceof IShellMetric)) {
            throw new RuntimeException("Metric["+name+"] is not IShellMetric, can not call by RPC");
        }
        IShellMetric iShellMetric = (IShellMetric)iMetric;

        //call updateMetricFromRPC with params
        Object paramsObj = shellMsg.getMetricParams();
        try {
            iShellMetric.updateMetricFromRPC(paramsObj);
        } catch (RuntimeException re) {
            throw re;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void setHeartbeat() {
        lastHeartbeatTimestamp.set(System.currentTimeMillis());
    }

    private long getLastHeartbeat() {
        return lastHeartbeatTimestamp.get();
    }

    protected void die(ShellBoltException exception) {
        String processInfo = _process.getProcessInfoString() + _process.getProcessTerminationInfoString();
        _exception = new RuntimeException(processInfo, exception);
        LOG.error("Halting process: ShellBolt died.", _exception);
        _collector.reportError(_exception);
        if (_running || (exception.getCause() instanceof Error)) { //don't exit if not running, unless it is an Error
            Runtime.getRuntime().halt(11);
        }
    }

    public Long getPid() {
        return pid;
    }

    @Override
    public void onMessage(ShellResponseMsg shellMsg) {
        String command = shellMsg.getCommand();
        if (command == null) {
            throw new IllegalArgumentException("Command not found in bolt message: " + shellMsg);
        }
        if (command.equals("sync")) {
            setHeartbeat();
        } else if(command.equals("ack")) {
            setHeartbeat();
            handleAck(shellMsg.getId());
        } else if (command.equals("fail")) {
            setHeartbeat();
            handleFail(shellMsg.getId());
        } else if (command.equals("error")) {
            handleError(shellMsg.getMsg());
        } else if (command.equals("log")) {
            handleLog(shellMsg);
        } else if (command.equals("emit")) {
            handleEmit(shellMsg);
        } else if (command.equals("metrics")) {
            handleMetrics(shellMsg);
        }
    }

    @Override
    public void onShellProcessExit(int exitCode) {
        die(new ShellProcessExitException(exitCode));
    }

    @Override
    public void onInternalError(Throwable t) {
        _collector.reportError(t);
    }

    private class BoltHeartbeatTimerTask extends TimerTask {
        private NuShellBolt bolt;

        public BoltHeartbeatTimerTask(NuShellBolt bolt) {
            this.bolt = bolt;
        }

        @Override
        public void run() {
            long currentTimeMillis = System.currentTimeMillis();
            long lastHeartbeat = getLastHeartbeat();

            if (lastHeartbeat > 0 && currentTimeMillis - lastHeartbeat > heartbeatTimeoutMills) {
                bolt.die(new HeartbeatException());
            }

            String genId = Long.toString(_rand.nextLong());
            _process.writeBoltMsg(createHeartbeatBoltMessage(genId));
        }

        private ShellBoltMsg createHeartbeatBoltMessage(String genId) {
            ShellBoltMsg msg = new ShellBoltMsg();
            msg.setId(genId);
            msg.setTask(Constants.SYSTEM_TASK_ID);
            msg.setStream(HEARTBEAT_STREAM_ID);
            msg.setTuple(new ArrayList<Object>());
            return msg;
        }

    }

    private static class HeartbeatThreadFactory implements ThreadFactory {

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("shell-bolt-heartbeat");
            return t;
        }
    }
}
