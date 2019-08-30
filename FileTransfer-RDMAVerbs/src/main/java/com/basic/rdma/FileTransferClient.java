package com.basic.rdma;

import com.basic.rdma.util.CmdLineCommon;
import com.ibm.disni.VerbsTools;
import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTransferClient {
    private static final Logger logger = LoggerFactory.getLogger(FileTransferClient.class);

    private CmdLineCommon cmdLineCommon;
    public FileTransferClient(CmdLineCommon cmdLineCommon) throws Exception {
        this.cmdLineCommon = cmdLineCommon;
    }

    /**
     * 连接RDMA文件传输服务器
     * @throws IOException
     * @throws InterruptedException
     */
    public void connect(String host, int port) throws IOException, InterruptedException {

    }

    /**
     * 客户端发送单个文件
     * @param filePath
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendSingleFile(String filePath) throws Exception {
        System.out.println("VerbsClient::starting...");
        //open the CM and the verbs interfaces

        //create a communication channel for receiving CM events
        //1.创建一个RDMA的Channel 通信通道为了接收RDMA 通信事件
        RdmaEventChannel cmChannel = RdmaEventChannel.createEventChannel();
        if (cmChannel == null){
            System.out.println("VerbsClient::cmChannel null");
            return;
        }

        //create a RdmaCmId for this client
        //2.创建一个RDMA 通信ID 为了client 每一个clinet就是一个RDMA client
        RdmaCmId idPriv = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (idPriv == null){
            System.out.println("VerbsClient::id null");
            return;
        }

        //before connecting, we have to resolve addresses
        //3.在建立连接之前，先要决定目标地址。
        InetAddress _dst = InetAddress.getByName(cmdLineCommon.getIp());
        InetSocketAddress dst = new InetSocketAddress(_dst, cmdLineCommon.getPort());
        int ret = idPriv.resolveAddr(null, dst, 2000);
        if (ret < 0){
            System.out.println("VerbsClient::resolveAddr failed");
            return;
        }

        //resolve addr returns an event, we have to catch that event
        //决定了目标地址会返回一个事件，我们必须catch 这个事件
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("VerbsClient::cmEvent null");
            return;
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ADDR_RESOLVED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            return;
        }
        cmEvent.ackEvent();

        //we also have to resolve the route
        //4.需要解决路由
        ret = idPriv.resolveRoute(2000);
        if (ret < 0){
            System.out.println("VerbsClient::resolveRoute failed");
            return;
        }

        //and catch that event too
        //catch 这个事件
        cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("VerbsClient::cmEvent null");
            return;
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ROUTE_RESOLVED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            return;
        }
        cmEvent.ackEvent();


        //let's create a device context
        //5.创建一个设备Context
        IbvContext context = idPriv.getVerbs();

        //and a protection domain, we use that one later for registering memory
        //6.创建一个保护域，用来注册内存Memory Region
        IbvPd pd = context.allocPd();
        if (pd == null){
            System.out.println("VerbsClient::pd null");
            return;
        }

        //the comp channel is used for getting CQ events
        //7.创建一个the comp channel，这个channel被用来获取CQ事件
        IbvCompChannel compChannel = context.createCompChannel();
        if (compChannel == null){
            System.out.println("VerbsClient::compChannel null");
            return;
        }

        //let's create a completion queue
        //8.让我们创建一个完成队列
        IbvCQ cq = context.createCQ(compChannel, 50, 0);
        if (cq == null){
            System.out.println("VerbsClient::cq null");
            return;
        }
        //and request to be notified for this queue
        cq.reqNotification(false).execute().free();

        //we prepare for the creation of a queue pair (QP)
        //9.创建一个Queue Pair
        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(10);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(10);
        attr.setQp_type(IbvQP.IBV_QPT_RC);
        attr.setRecv_cq(cq);
        attr.setSend_cq(cq);
        //let's create a queue pair
        IbvQP qp = idPriv.createQP(pd, attr);
        if (qp == null){
            System.out.println("VerbsClient::qp null");
            return;
        }

        ///////////////////////////////////////////////////////准备工作////////////////////////////////////////////////////////////////////
        int buffercount = 1;
        int buffersize = cmdLineCommon.getSize();
        ByteBuffer buffers[] = new ByteBuffer[1];
        IbvMr dataMr = null;
        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;

        //before we connect we also want to register some buffers
        //register some buffers to be used later
        buffers[0] = ByteBuffer.allocateDirect(buffersize);
        dataMr = pd.regMr(buffers[0], access).execute().free().getMr();
        ByteBuffer dataBuf = buffers[0];

        //now let's connect to the server
        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
        ret = idPriv.connect(connParam);
        if (ret < 0){
            System.out.println("VerbsServer::accept failed");
            return;
        }
        //wait until the connection is officially switched into established mode
        cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED
                .ordinal()) {
            System.out.println("VerbsServer::wrong event received: " + cmEvent.getEvent());
            return;
        }
        //always ack CM events
        cmEvent.ackEvent();

        // write data File
        File file= new File(filePath);
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        randomAccessFile.getChannel().read(dataBuf);

        //this class is a thin wrapper over some of the data operations in jverbs
        //we use it to issue data transfer operations
        VerbsTools commRdma = new VerbsTools(context, compChannel, qp, cq);
        LinkedList<IbvSendWR> wrList_send = new LinkedList<IbvSendWR>();

        //let's preopare some work requests for sending
        // 应用程序发送RDMA SEND请求到RNIC
        IbvSge sgeSend = new IbvSge();
        sgeSend.setAddr(dataMr.getAddr());
        sgeSend.setLength(dataMr.getLength());
        sgeSend.setLkey(dataMr.getLkey());
        LinkedList<IbvSge> sgeList = new LinkedList<IbvSge>();
        sgeList.add(sgeSend);
        IbvSendWR sendWR = new IbvSendWR();
        sendWR.setWr_id(2000);
        sendWR.setSg_list(sgeList);
        sendWR.setOpcode(IbvSendWR.IBV_WR_SEND);
        sendWR.setSend_flags(IbvSendWR.IBV_SEND_SIGNALED);
        wrList_send.add(sendWR);

        //post a send call, here we send a message which include the RDMA information of a data buffer
        // 应用程序发送 RDMA SEND请求到Send Queue，同时等待Complete Queue中请求执行完成
        commRdma.send(buffers, wrList_send, true, false);
        System.out.println("VerbsServer::stag info sent");
        randomAccessFile.getChannel().close();
    }
}
