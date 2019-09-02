package com.basic.rdma.main;

import com.basic.rdma.FileTransferOriginClient;
import com.basic.rdma.util.CmdLineCommon;
import org.apache.commons.cli.ParseException;

/**
 * locate com.basic.rdma.main
 * Created by master on 2019/8/25.
 * java -cp FileTransfer-RDMAVerbs-1.0-SNAPSHOT-jar-with-dependencies.jar com.basic.rdma.main.RDMAFileTransferOriginClient -a -p -s -f
 */
public class RDMAFileTransferOriginClient {
    private FileTransferOriginClient transferClient;
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
        this.transferClient = new FileTransferOriginClient(cmdLine);
        this.run();
    }

    public static void main(String[] args) throws Exception {
        RDMAFileTransferOriginClient simpleClient = new RDMAFileTransferOriginClient();
        simpleClient.launch(args);
    }
}

