package com.basic.rdma.main;

import com.basic.rdma.DirectorySequenceTransferClient;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp DirectoryTransfer-RDMAChannel-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMASequenceDirectoryTransferClient -a -p -s -f -i
 */
public class RDMASequenceDirectoryTransferClient {
    private DirectorySequenceTransferClient transferClient;
    private CmdLineCommon cmdLine;

    public void run() throws Exception {
        transferClient.connect(cmdLine.getIp(),cmdLine.getPort());
        transferClient.sendSingleDirectory(cmdLine.getPath());
    }

    public void launch(String[] args) throws Exception {
        this.cmdLine = new CmdLineCommon("RDMASequenceDirectoryTransferClient");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        this.transferClient = new DirectorySequenceTransferClient(cmdLine,new RdmaChannelConf());
        this.run();
        transferClient.stop();
    }

    public static void main(String[] args) throws Exception {
        RDMASequenceDirectoryTransferClient simpleClient = new RDMASequenceDirectoryTransferClient();
        simpleClient.launch(args);
    }
}

