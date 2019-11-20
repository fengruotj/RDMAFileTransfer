package com.basic.rdma;

import com.basic.rdma.task.FileTranserHandlerTask;
import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdmachannel.channel.RdmaChannel;
import com.basic.rdmachannel.channel.RdmaChannelConf;
import com.basic.rdmachannel.channel.RdmaConnectListener;
import com.basic.rdmachannel.channel.RdmaNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTransferServer implements RdmaConnectListener {
    private static final Logger logger = LoggerFactory.getLogger(FileTransferServer.class);
    private RdmaNode rdmaServer;

    private ExecutorService executorService;

    private CmdLineCommon cmdLineCommon;
    public FileTransferServer(CmdLineCommon cmdLineCommon, RdmaChannelConf rdmaChannelConf) throws Exception {
        String hostName = cmdLineCommon.getIface();
        this.cmdLineCommon = cmdLineCommon;
        this.rdmaServer = new RdmaNode(hostName, cmdLineCommon.getPort(), rdmaChannelConf, RdmaChannel.RdmaChannelType.RPC);
        this.executorService = Executors.newCachedThreadPool();
    }

    /**
     * FileTransferServer 绑定连接，并且注册完成时间监听
     *
     * @throws Exception
     */
    public void bind() throws Exception {
        rdmaServer.bindConnectCompleteListener(this);
    }

    public void stop() throws Exception {
        this.rdmaServer.stop();
        this.executorService.shutdown();
    }

    @Override
    public void onSuccess(InetSocketAddress inetSocketAddress, RdmaChannel rdmaChannel) {
        logger.info("success accept RdmaChannel: " + inetSocketAddress.getHostName());
        executorService.submit(new FileTranserHandlerTask(cmdLineCommon, rdmaChannel, rdmaServer.getRdmaBufferManager()));
    }

    @Override
    public void onFailure(Throwable exception) {
        exception.printStackTrace();
    }

}
