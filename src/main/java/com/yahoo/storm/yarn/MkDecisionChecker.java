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
public class MkDecisionChecker extends Thread {
    private static Logger LOG = LoggerFactory.getLogger(MkDecisionChecker.class);
    private Map<String, Object> stormConf;
    private final StormMasterServerHandler master;
    private Nimbus.Iface nimbus;
    //private int executorPerWorker;

    public MkDecisionChecker(Map<String, Object> stormConf, StormMasterServerHandler master) {
        this.stormConf = stormConf;
        //this.executorPerWorker = executorPerWorker;
        this.master = master;
        setDaemon(true);
        setName("storm cluster checker by executor thread");
    }

    @Override
    public void run() {
        LOG.info("MkDecisionChecker try to connect storm nimbus");
        while (true) {
            while (nimbus == null) {
                try {
                    Thread.sleep(10000);
                    nimbus = NimbusClient.getConfiguredClient(stormConf).getClient();
                    LOG.info("Connected to storm nimbus, start MkDecisionChecker...");
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

                int totalSupervisors = stormCluster.get_supervisors_size();
                int totalNumWorkers = stormCluster.get_supervisors().stream()
                        .mapToInt(SupervisorSummary::get_num_workers).sum();
                int totalUsedWorkers = stormCluster.get_topologies().stream().mapToInt(TopologySummary::get_num_workers).sum();
                //int totalUsedExecutors = stormCluster.get_topologies().stream()
                        //.mapToInt(TopologySummary::get_num_executors).sum() - totalUsedWorkers - 1;
                //int totalNumExecutors = executorPerWorker * totalNumWorkers;
                LOG.info("1:"+stormCluster.toString());
                LOG.info("test output: "+totalSupervisors+" : "+totalNumWorkers+" : "+totalUsedWorkers);
                if (totalUsedWorkers >= totalNumWorkers) {
                    if(stormCluster.get_topologies().size() != 0 || stormCluster.get_supervisors().size() == 0) {
                        LOG.info("case ADD: totalNumWorkers:" + totalNumWorkers + " and totalUsedWorkers:" + totalUsedWorkers);
                        LOG.info("Need more workers, add 1 supervisor");
                        master.addSupervisors(1);
                        LOG.info("ADD:"+stormCluster.toString());
                    }
                } else {
                    int oneSupervisorWorkersNum = stormCluster.get_supervisors().get(0).get_num_workers();
                    int numOfVacant = (totalNumWorkers - totalUsedWorkers);
                    if (numOfVacant > oneSupervisorWorkersNum && stormCluster.get_supervisors().size() > 1) {
                        LOG.info("case REMOVE: totalNumWorkers:" + totalNumWorkers + " and totalUsedWorkers:" + totalUsedWorkers);
                        Iterator<Container> it = master.getContainerInfo().iterator();
                        String containerID;
                        if (it.hasNext()) {
                            containerID = it.next().getId().toString();
                            master.removeSupervisors(containerID);
                            it.remove();
                            LOG.info("remove a supervisor " + containerID);
                        }
                        LOG.info("REMOVE:"+stormCluster.toString());
                    }

                }

            } catch (TException e) {
                nimbus = null;
            } //catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
