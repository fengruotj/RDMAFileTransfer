package com.basic.rdma.util;

import org.junit.Test;

/**
 * locate com.basic.rdma.util
 * Created by master on 2019/8/25.
 */
public class CmdLineCommonTest {

    @Test
    public void Test(){
        String args[]=new String[]{"-a","192.168.1.1","-p","22","-s","1024","-f","/Users/master/Downloads/Termius.dmg","-i","ib0"};
        CmdLineCommon cmdLine = new CmdLineCommon("CmdLineCommonTest");
        try {
            cmdLine.parse(args);
        } catch (Exception e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        System.out.println("ip Host: "+ cmdLine.getIp());
        System.out.println("Port : "+ cmdLine.getPort());
        System.out.println("Size : "+ cmdLine.getSize());
        System.out.println("Path : "+ cmdLine.getPath());
        System.out.println("Iface : "+ cmdLine.getIface());
    }
}
