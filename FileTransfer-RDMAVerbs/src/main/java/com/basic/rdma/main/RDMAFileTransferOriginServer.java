package com.basic.rdma.main;

import com.basic.rdma.FileTransferOriginServer;
import com.basic.rdma.util.CmdLineCommon;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-RDMAVerbs-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMAFileTransferOriginServer -a -p -s -f
 */
public class RDMAFileTransferOriginServer {
    private FileTransferOriginServer transferServer;
    private CmdLineCommon cmdLine;

    public void run() throws Exception {
        transferServer.recvSingleFile(cmdLine.getPath());
    }

    public void launch(String[] args) throws Exception {
        this.cmdLine = new CmdLineCommon("RDMAFileTransferServer");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        this.transferServer = new FileTransferOriginServer(cmdLine);
        this.run();
    }

    public static void main(String[] args) throws Exception {
        RDMAFileTransferOriginServer simpleServer = new RDMAFileTransferOriginServer();
        simpleServer.launch(args);
    }
}

