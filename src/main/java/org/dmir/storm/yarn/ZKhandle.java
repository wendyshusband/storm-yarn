package org.dmir.storm.yarn;

import org.apache.storm.shade.org.apache.curator.RetryPolicy;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.storm.shade.org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by 44931 on 2018/3/5.
 */
public class ZKhandle {
    private static CuratorFramework client = null;
    public final static ExecutorService EXECUTOR_SERVICE = Executors.newCachedThreadPool();
    public static final char PATH_SEPARATOR_CHAR = '/';
    private static Map<String, NodeCache> nodeCaches = new HashMap<>();

    private ZKhandle(String zkServer, int port, int sessionTimeoutMs,
                         int connectionTimeoutMs, int baseSleepTimeMsint, int maxRetries) {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(baseSleepTimeMsint, maxRetries);
        client = CuratorFrameworkFactory.builder().connectString(zkServer+":"+port).retryPolicy(retryPolicy)
                .sessionTimeoutMs(sessionTimeoutMs).connectionTimeoutMs(connectionTimeoutMs).build();

    }

    public static synchronized CuratorFramework newClient(String zkServer, int port, int sessionTimeoutMs,
                                                          int connectionTimeoutMs, int baseSleepTimeMsint, int maxRetries) {
        if (client == null) {
            new ZKhandle(zkServer, port, sessionTimeoutMs,
                    connectionTimeoutMs, baseSleepTimeMsint, maxRetries);
        }
        return client;
    }

    public static boolean clientIsStart() {
        return client.isStarted();
    }

    public static void start() throws Exception {
        client.start();
    }

    public static void close(String path) throws Exception {
        nodeCaches.values().stream().forEach(e -> {
            try {
                e.close();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        });
        EXECUTOR_SERVICE.shutdownNow();
        client.close();
    }

    public static NodeCache createNodeCache (String path) throws Exception {
        if (nodeCaches.containsKey(path)) {
            return nodeCaches.get(path);
        } else {
            NodeCache nodeCache = new NodeCache(client, path);
            nodeCaches.put(path, nodeCache);
            nodeCaches.put(path, nodeCache);
            nodeCache.start();
            return nodeCache;
        }
    }

    public static synchronized void sentResourceResponse(int response) throws Exception {
        //response = 1 success , = 0 fail.
        if (!clientIsStart()) {
            ZKhandle.start();
        }
        if (client.checkExists().forPath("/resource/") == null) {
            client.create().creatingParentsIfNeeded().forPath("/resource/response", String.valueOf(response).getBytes());
        } else {
            client.setData().forPath("/resource/response", String.valueOf(response).getBytes());
        }
    }

    public static String[] getNeedRemoveSupervisor(int number) throws Exception {
        String[] needRemoveSupervisor = new String[number];
        if (client.checkExists().forPath("/resource/sub") != null) {
            String nodesStr = new String(client.getData().forPath("/resource/sub"));
            nodesStr = nodesStr.substring(1, nodesStr.length()-1);
            needRemoveSupervisor = nodesStr.split(", ");
        }
        return needRemoveSupervisor;
    }
}
