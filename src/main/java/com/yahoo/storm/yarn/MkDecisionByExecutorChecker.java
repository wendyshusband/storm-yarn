package com.yahoo.storm.yarn;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.storm.generated.ClusterSummary;
import org.apache.storm.generated.Nimbus;
import org.apache.storm.generated.SupervisorSummary;
import org.apache.storm.generated.TopologySummary;
import org.apache.storm.thrift.TException;
import org.apache.storm.utils.NimbusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by kailin on 1/5/17.
 */
public class MkDecisionByExecutorChecker extends Thread {
    private static Logger LOG = LoggerFactory.getLogger(MkDecisionByExecutorChecker.class);
    private Map<String, Object> stormConf;
    private final StormMasterServerHandler master;
    private Nimbus.Iface nimbus;
    private int executorPerWorker;

    public MkDecisionByExecutorChecker(int executorPerWorker,Map<String, Object> stormConf, StormMasterServerHandler master) {
        this.stormConf = stormConf;
        this.executorPerWorker = executorPerWorker;
        this.master = master;
        setDaemon(true);
        setName("storm cluster checker by executor thread");
    }

    @Override
    public void run() {
        LOG.info("MkDecisionByExecutorChecker try to connect storm nimbus");
        while (true) {
            while (nimbus == null) {
                try {
                    Thread.sleep(10000);
                    nimbus = NimbusClient.getConfiguredClient(stormConf).getClient();
                    LOG.info("Connected to storm nimbus, start MkDecisionByExecutorChecker...");
                } catch (Exception e) {
                }
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
            try {
                ClusterSummary stormCluster = nimbus.getClusterInfo();
                int totalNumWorkers = stormCluster.get_supervisors().stream()
                        .mapToInt(SupervisorSummary::get_num_workers).sum();
                int totalUsedWorkers = stormCluster.get_topologies().stream().mapToInt(TopologySummary::get_num_workers).sum();
                int totalUsedExecutors = stormCluster.get_topologies().stream()
                        .mapToInt(TopologySummary::get_num_executors).sum() - totalUsedWorkers - 1;
                int totalNumExecutors = executorPerWorker * totalNumWorkers;
                LOG.info("totalNumWorkers:" + totalNumWorkers+" and totalUsedWorkers:" + totalUsedWorkers+"" +
                        " totalUsedExecutors:" + totalUsedExecutors + " and totalNumExecutors:" + totalNumExecutors);
                if (totalUsedExecutors >= totalNumExecutors) {
                    if(totalUsedWorkers >= totalNumWorkers) {
                        LOG.info("Need more workers, add 1 supervisor");
                        master.addSupervisors(1);
                    }else{
                        if((totalUsedExecutors - totalNumExecutors)/executorPerWorker > 0)
                            LOG.info("Do not need add a supervisor, just add "
                                    +(totalUsedExecutors - totalNumExecutors)/executorPerWorker+" worker.");
                    }
                }else{
                    int oneSupervisorWorkersNum = stormCluster.get_supervisors().get(0).get_num_workers();
                    int numOfVacant = (totalNumWorkers - totalUsedWorkers) ;
                    if(numOfVacant > oneSupervisorWorkersNum){
                        Iterator<Container> it = master.getContainerInfo().iterator();
                        String containerID;
                        if (it.hasNext()) {
                            containerID = it.next().getId().toString();
                            master.removeSupervisors(containerID);
                            LOG.info("remove a supervisor " + containerID);
                        }
                    }
                }
            } catch (TException e) {
                nimbus = null;
            }
        }
    }
}
