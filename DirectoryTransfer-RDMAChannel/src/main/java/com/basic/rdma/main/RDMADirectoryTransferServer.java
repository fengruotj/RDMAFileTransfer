package com.basic.rdma.main;

import com.basic.rdma.DirectoryTransferServer;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-RDMAChannel-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMADirectoryTransferServer -a -p -s -f -i
 */
public class RDMADirectoryTransferServer {
    private DirectoryTransferServer transferServer;
    private CmdLineCommon cmdLine;

    public void run() throws Exception {
        transferServer.bind();
    }

    public void launch(String[] args) throws Exception {
        this.cmdLine = new CmdLineCommon("RDMADirectoryTransferServer");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        this.transferServer = new DirectoryTransferServer(cmdLine,new RdmaChannelConf());
        this.run();
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        RDMADirectoryTransferServer simpleServer = new RDMADirectoryTransferServer();
        simpleServer.launch(args);
    }
}

