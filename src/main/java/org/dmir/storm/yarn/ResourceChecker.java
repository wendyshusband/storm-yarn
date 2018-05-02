package org.dmir.storm.yarn;

import org.apache.storm.generated.Nimbus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by 44931 on 2018/3/5.
 */
public class ResourceChecker extends Thread {
    private static Logger LOG = LoggerFactory.getLogger(ResourceChecker.class);
    private Map<String, Object> stormConf;
    private final StormMasterServerHandler master;
    private Nimbus.Iface nimbus;

    public ResourceChecker(Map<String, Object> stormConf, StormMasterServerHandler master) {
        this.stormConf = stormConf;
        this.master = master;
        setDaemon(true);
        setName("storm cluster resource checker thread");
    }

    @Override
    public void run() {
        super.run();
    }
}
