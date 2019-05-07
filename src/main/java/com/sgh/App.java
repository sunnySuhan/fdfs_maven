package com.sgh;

import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;

import java.io.FileOutputStream;


public class App {
    //  获取客户端
    public static StorageClient getClient() throws Exception {
        // 加载配置文件
        ClientGlobal.init("fdfs_client.conf");
        TrackerClient trackerClient = new TrackerClient();
        TrackerServer trackerServer = trackerClient.getConnection();
        // 通过client对象操作分布式文件系统
        StorageClient storageClient = new StorageClient(trackerServer, null);
        return storageClient;
    }

    // 上传操作
    public static void uploadFile(String fileName, String ext, NameValuePair[] nameValuePair) throws Exception {
        getClient().upload_file(fileName, ext, nameValuePair);
    }

    //  下载
    public static void downloadFile(String groupName, String remoteFileName, String savePathAndName) throws Exception {
        byte[] file = getClient().download_file(groupName, remoteFileName);
        FileOutputStream fileOutputStream = new FileOutputStream(savePathAndName);
        fileOutputStream.write(file);
        fileOutputStream.close();
    }

    //  删除
    public static int deleteFile(String group, String remoteFileName) throws Exception {
        int i = getClient().delete_file(group, remoteFileName);
        return i;
    }

    //  获得文件信息
    public static void getFileInfo(String group, String remoteFileName) throws Exception {
        FileInfo file_info = getClient().get_file_info(group, remoteFileName);
        System.out.println("size:  " + file_info.getFileSize());
        System.out.println("from:  " + file_info.getSourceIpAddr());
        System.out.println("createTime:  " + file_info.getCreateTimestamp());
    }

    //  获取元数据信息
    public static void getMetaData(String group, String remoteFileName) throws Exception {

        NameValuePair[] metadata = getClient().get_metadata(group, remoteFileName);
        for (NameValuePair metadatum : metadata) {
            System.out.println(metadatum.getName() + ":  " + metadatum.getValue());
        }
    }

    public static void main(String[] args) throws Exception {
        //测试上传
        // uploadFile("D:\\水.jpg","jpg",new NameValuePair[]{new NameValuePair("from","sgh")});

        // 测试下载
        // downloadFile("group1","M00/00/00/wKhfClzQ34yAHTbFAArv1KfunAg753.jpg","D:\\c.jpg");

        //测试删除
        //int group1 = deleteFile("group1", "M00/00/00/wKhfClzQ34yAHTbFAArv1KfunAg753.jpg");
        //System.err.println(group1);


        // 测试查看文件信息
        //getFileInfo("group1","M00/00/00/wKhfClzQ46uAAo7LAArv1KfunAg022.jpg");


        //  测试获取文件元数据信息
        getMetaData("group1", "M00/00/00/wKhfClzQ46uAAo7LAArv1KfunAg022.jpg");


    }
}
