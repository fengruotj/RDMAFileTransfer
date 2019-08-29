package com.basic.rdma.main;

import com.basic.rdma.FileTransferServer;
import com.basic.rdma.util.CmdLineCommon;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-Verbs-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMAFileTransferServer -a -p -s -f
 */
public class RDMAFileTransferServer {
    private FileTransferServer transferServer;
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
        this.transferServer = new FileTransferServer(cmdLine);
        this.run();
        Thread.sleep(Integer.MAX_VALUE);
    }

    public static void main(String[] args) throws Exception {
        RDMAFileTransferServer simpleServer = new RDMAFileTransferServer();
        simpleServer.launch(args);
    }
}

