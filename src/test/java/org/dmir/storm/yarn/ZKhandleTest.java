package org.dmir.storm.yarn;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by 44931 on 2018/3/5.
 */
public class ZKhandleTest {
    @Test
    public void getNeedRemoveSupervisor() {
        List<String> nodes = new ArrayList<>();
        nodes.add("node1");
        nodes.add("node2");
        nodes.add("node3");
        System.out.println(nodes.toString());
        String bnode = new String(nodes.toString().getBytes());
        System.out.println(bnode);
        bnode = bnode.substring(1, bnode.length() - 1);
        System.out.println(bnode);
        String[] a = new String[4];
        a = bnode.split(", ");
        for  (int i=0; i<a.length; i++) {
            System.out.println(a[i]);
        }
        System.out.println(a.length);
    }
}