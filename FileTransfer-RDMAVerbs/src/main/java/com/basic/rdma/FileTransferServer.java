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
        attr.cap().setMax_recv_wr(4096);
        attr.cap().setMax_send_sge(1);
        attr.cap().setMax_send_wr(4096);
        attr.setQp_type(IbvQP.IBV_QPT_RC);
        attr.setRecv_cq(cq);
        attr.setSend_cq(cq);
        //create the queue pair for the client connection
        IbvQP qp = connId.createQP(pd, attr);
        if (qp == null){
            System.out.println("VerbsServer::qp null");
            return;
        }

        //////////////////////////////////////data index transferSize/////////////////////////////////
        int buffercount = 2;
        ByteBuffer buffers[] = new ByteBuffer[buffercount];
        int access = IbvMr.IBV_ACCESS_LOCAL_WRITE | IbvMr.IBV_ACCESS_REMOTE_WRITE | IbvMr.IBV_ACCESS_REMOTE_READ;
        //before we connect we also want to register some buffers
        //register some buffers to be used later
        buffers[0] = ByteBuffer.allocateDirect(Constants.INFOBUFFER_SIZE);
        buffers[1] = ByteBuffer.allocateDirect(cmdLineCommon.getSize()+ Constants.BLOCKINDEX_SIZE + Constants.BLOCKLENGTH_SIZE);
        IbvMr infoMr = pd.regMr(buffers[0], access).execute().free().getMr();
        IbvMr dataMr = pd.regMr(buffers[1], access).execute().free().getMr();
        ByteBuffer infoByteBuffer = buffers[0];
        ByteBuffer dataByteBuffer = buffers[1];

        //once the client id is set up, accept the connection
        RdmaConnParam connParam = new RdmaConnParam();
        connParam.setRetry_count((byte) 2);
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

        //////////////////////////////////////File Information//////////////////////////////////////
        int splitSize=0;
        long fileLength=0L;

        LinkedList<IbvRecvWR> wrInfoList_recv = new LinkedList<IbvRecvWR>();
        IbvSge sgeInfoRecv = new IbvSge();
        sgeInfoRecv.setAddr(infoMr.getAddr());
        sgeInfoRecv.setLength(infoMr.getLength());
        sgeInfoRecv.setLkey(infoMr.getLkey());
        LinkedList<IbvSge> sgeInfoListRecv = new LinkedList<IbvSge>();
        sgeInfoListRecv.add(sgeInfoRecv);
        IbvRecvWR recvInfoWR = new IbvRecvWR();
        recvInfoWR.setSg_list(sgeInfoListRecv);
        recvInfoWR.setWr_id(0);
        wrInfoList_recv.add(recvInfoWR);

        VerbsTools commRdma = new VerbsTools(context, compChannel, qp, cq);
        commRdma.initSGRecv(wrInfoList_recv);
        //let's wait for the first message to be received from the server
        // 应用程序发送 RDMA RECEIVE请求到Receive Queue，同时等待Complete Queue中请求执行完成
        commRdma.completeSGRecv(wrInfoList_recv, false);
        splitSize = infoByteBuffer.getInt();
        fileLength = infoByteBuffer.getLong();
        logger.info("Transfer Split File {} Block , Filelength {}", splitSize, fileLength);

        //////////////////////////////////////init File/////////////////////////////////
        File file= new File(filePath);
        if(file.exists())
            file.delete();
        RandomAccessFile randomAccessFile=new RandomAccessFile(file, "rw");
        FileChannel fileChannel = randomAccessFile.getChannel();

        //////////////////////////////////////write data File//////////////////////////////////////
        for (int i = 0; i < splitSize; i++) {
            dataByteBuffer.clear();

            LinkedList<IbvRecvWR> wrDataList_recv = new LinkedList<IbvRecvWR>();
            IbvSge sgeDataRecv = new IbvSge();
            sgeDataRecv.setAddr(dataMr.getAddr());
            sgeDataRecv.setLength(dataMr.getLength());
            sgeDataRecv.setLkey(dataMr.getLkey());
            LinkedList<IbvSge> sgeDataListRecv = new LinkedList<IbvSge>();
            sgeDataListRecv.add(sgeDataRecv);
            IbvRecvWR recvDataWR = new IbvRecvWR();
            recvDataWR.setSg_list(sgeDataListRecv);
            recvDataWR.setWr_id(i+1001);
            wrDataList_recv.add(recvDataWR);

            VerbsTools commDataRdma = new VerbsTools(context, compChannel, qp, cq);
            commDataRdma.initSGRecv(wrDataList_recv);
            //let's wait for the first message to be received from the server
            // 应用程序发送 RDMA RECEIVE请求到Receive Queue，同时等待Complete Queue中请求执行完成
            commDataRdma.completeSGRecv(wrDataList_recv, false);

            int index = dataByteBuffer.getInt();
            long length = dataByteBuffer.getLong();

            logger.info("BLOCK {} RECEIVE Success!!! : {}", index, length);
            dataByteBuffer.limit((int) (length + Constants.BLOCKINDEX_SIZE+ Constants.BLOCKLENGTH_SIZE));
            while(dataByteBuffer.hasRemaining()){
                fileChannel.write(dataByteBuffer);
            }
        }
    }

}
