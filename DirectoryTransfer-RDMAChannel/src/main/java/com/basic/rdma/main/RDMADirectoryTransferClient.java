package com.basic.rdma.main;

import com.basic.rdma.DirectoryTransferClient;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-RDMAChannel-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMADirectoryTransferClient -a -p -s -f -i
 */
public class RDMADirectoryTransferClient {
    private DirectoryTransferClient transferClient;
    private CmdLineCommon cmdLine;

    public void run() throws Exception {
        transferClient.connect(cmdLine.getIp(),cmdLine.getPort());
        transferClient.sendSingleFile(cmdLine.getPath());
    }

    public void launch(String[] args) throws Exception {
        this.cmdLine = new CmdLineCommon("RDMADirectoryTransferClient");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        this.transferClient = new DirectoryTransferClient(cmdLine,new RdmaChannelConf());
        this.run();
        transferClient.stop();
    }

    public static void main(String[] args) throws Exception {
        RDMADirectoryTransferClient simpleClient = new RDMADirectoryTransferClient();
        simpleClient.launch(args);
    }
}

