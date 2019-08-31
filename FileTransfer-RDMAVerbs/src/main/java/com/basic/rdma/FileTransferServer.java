package com.basic.rdma;

import com.basic.rdma.util.CmdLineCommon;
import com.basic.rdma.util.RDMAUtils;
import com.ibm.disni.VerbsTools;
import com.ibm.disni.rdma.verbs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.LinkedList;

/**
 * locate com.basic.rdma
 * Created by master on 2019/8/25.
 */
public class FileTransferServer {
    private static final Logger logger = LoggerFactory.getLogger(FileTransferServer.class);

    private CmdLineCommon cmdLineCommon;
    public FileTransferServer(CmdLineCommon cmdLineCommon) throws Exception {
        this.cmdLineCommon = cmdLineCommon;
    }

    public void bind() {

    }

    /**
     * 接受单个文件传输
     * @param filePath
     * @throws Exception
     */
    public void recvSingleFile(String filePath) throws Exception {
        System.out.println("VerbsServer::starting...");

        //create a communication channel for receiving CM events
        //1.创建一个RDMA的Channel 通信通道为了接收RDMA 通信事件
        RdmaEventChannel cmChannel = RdmaEventChannel.createEventChannel();
        if (cmChannel == null){
            System.out.println("VerbsServer::CM channel null");
            return;
        }

        //create a RdmaCmId for the server
        //2.创建一个RDMA 通信ID 为了client 每一个clinet就是一个RDMA client
        RdmaCmId idPriv = cmChannel.createId(RdmaCm.RDMA_PS_TCP);
        if (idPriv == null){
            System.out.println("idPriv null");
            return;
        }

        // 3. 绑定IP地址和端口，监听Channel
        InetAddress _src = RDMAUtils.getLocalHostLANAddress(cmdLineCommon.getIface());
        InetSocketAddress src = new InetSocketAddress(_src, cmdLineCommon.getPort());
        int ret = idPriv.bindAddr(src);
        if (ret < 0){
            System.out.println("VerbsServer::binding not sucessfull");
        }

        //listen on the id
        ret = idPriv.listen(10);
        if (ret < 0){
            System.out.println("VerbsServer::listen not successfull");
        }

        //wait for new connect requests
        RdmaCmEvent cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("cmEvent null");
            return;
        }
        else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_CONNECT_REQUEST
                .ordinal()) {
            System.out.println("VerbsServer::wrong event received: " + cmEvent.getEvent());
            return;
        }
        //always acknowledge CM events
        cmEvent.ackEvent();

        //get the id of the newly connection
        RdmaCmId connId = cmEvent.getConnIdPriv();
        if (connId == null){
            System.out.println("VerbsServer::connId null");
            return;
        }

        //get the device context of the new connection, typically the same as with the server id
        //5.创建一个设备Context
        IbvContext context = connId.getVerbs();
        if (context == null){
            System.out.println("VerbsServer::context null");
            return;
        }

        //create a new protection domain, we will use the pd later when registering memory
        //6.创建一个保护域，用来注册内存Memory Region
        IbvPd pd = context.allocPd();
        if (pd == null){
            System.out.println("VerbsServer::pd null");
            return;
        }

        //the comp channel is used to get CQ notifications
        IbvCompChannel compChannel = context.createCompChannel();
        if (compChannel == null){
            System.out.println("VerbsServer::compChannel null");
            return;
        }

        //create a completion queue
        IbvCQ cq = context.createCQ(compChannel, 50, 0);
        if (cq == null){
            System.out.println("VerbsServer::cq null");
            return;
        }
        //request to be notified on that CQ
        cq.reqNotification(false).execute().free();

        //prepare a new queue pair
        IbvQPInitAttr attr = new IbvQPInitAttr();
        attr.cap().setMax_recv_sge(1);
        attr.cap().setMax_recv_wr(10);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(10);
        attr.setQp_type(IbvQP.IBV_QPT_RC);
        attr.setRecv_cq(cq);
        attr.setSend_cq(cq);
        //create the queue pair for the client connection
        IbvQP qp = connId.createQP(pd, attr);
        if (qp == null){
            System.out.println("VerbsServer::qp null");
            return;
        }

        ///////////////////////////////////////////////////////准备工作////////////////////////////////////////////////////////////////////
        int buffercount = 1;
        ByteBuffer buffers[] = new ByteBuffer[buffercount];
        IbvMr dataMr = null;
        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;

        //before we connect we also want to register some buffers
        //register some buffers to be used later
        buffers[0] = ByteBuffer.allocateDirect(cmdLineCommon.getSize());
        dataMr = pd.regMr(buffers[0], access).execute().free().getMr();
        ByteBuffer dataBuf = buffers[0];

        LinkedList<IbvRecvWR> wrList_recv = new LinkedList<IbvRecvWR>();
        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
        //once the client id is set up, accept the connection
        ret = connId.accept(connParam);
        if (ret < 0){
            System.out.println("VerbsClient::connect failed");
            return;
        }

        //wait until we are really connected
        cmEvent = cmChannel.getCmEvent(-1);
        if (cmEvent == null){
            System.out.println("VerbsClient::cmEvent null");
            return;
        } else if (cmEvent.getEvent() != RdmaCmEvent.EventType.RDMA_CM_EVENT_ESTABLISHED
                .ordinal()) {
            System.out.println("VerbsClient::wrong event received: " + cmEvent.getEvent());
            return;
        }
        cmEvent.ackEvent();

        IbvSge sgeRecv = new IbvSge();
        sgeRecv.setAddr(dataMr.getAddr());
        sgeRecv.setLength(dataMr.getLength());
        sgeRecv.setLkey(dataMr.getLkey());
        LinkedList<IbvSge> sgeListRecv = new LinkedList<IbvSge>();
        sgeListRecv.add(sgeRecv);
        IbvRecvWR recvWR = new IbvRecvWR();
        recvWR.setSg_list(sgeListRecv);
        recvWR.setWr_id(1000);
        wrList_recv.add(recvWR);

        //it's important to post those receive operations before connecting
        //otherwise the server may issue a send operation and which cannot be received
        //this class wraps soem of the RDMA data operations
        VerbsTools commRdma = new VerbsTools(context, compChannel, qp, cq);
        commRdma.initSGRecv(wrList_recv);

        //let's wait for the first message to be received from the server
        // 应用程序发送 RDMA RECEIVE请求到Receive Queue，同时等待Complete Queue中请求执行完成
        commRdma.completeSGRecv(wrList_recv, false);

        //here we go, it contains the RDMA information of a remote buffer
        dataBuf.clear();

        // READ File Data
        File file= new File(filePath);
        if(file.exists())
            file.delete();
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();

        while(dataBuf.hasRemaining()){
            fileChannel.write(dataBuf);
        }

        dataBuf.clear();
        System.out.println("VerbsServer::stag info receive");

        fileChannel.close();
    }

}
