package com.basic.rdma.main;

import com.basic.rdma.FileTransferClient;
import com.basic.rdma.util.CmdLineCommon;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-RDMAVerbs-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMAFileTransferClient -a -p -s -f
 */
public class RDMAFileTransferClient {
    private FileTransferClient transferClient;
    private CmdLineCommon cmdLine;

    public void run() throws Exception {
        transferClient.sendSingleFile(cmdLine.getPath());
    }

    public void launch(String[] args) throws Exception {
        this.cmdLine = new CmdLineCommon("RDMAFileTransferClient");

        try {
            cmdLine.parse(args);
        } catch (ParseException e) {
            cmdLine.printHelp();
            System.exit(-1);
        }
        this.transferClient = new FileTransferClient(cmdLine);
        this.run();
    }

    public static void main(String[] args) throws Exception {
        RDMAFileTransferClient simpleClient = new RDMAFileTransferClient();
        simpleClient.launch(args);
    }
}

