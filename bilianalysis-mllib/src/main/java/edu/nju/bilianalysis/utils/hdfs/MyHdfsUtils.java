package edu.nju.bilianalysis.utils.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MyHdfsUtils {
    public static FileSystem getFileSystem() {
        try {
            Configuration config = new Configuration();
            config.set("fs.hdfs.impl",org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            URI uri = new URI("hdfs://172.19.240.230:9000/");
            return FileSystem.get(uri, config, "hdfs");// 第一位为uri，第二位为config，第三位是登录的用户
        }catch (URISyntaxException | IOException | InterruptedException e){
            e.printStackTrace();
        }
        return null;
    }
    public static boolean checkFileExist(FileSystem hdfs, String filename) {
        try {
            Path path = new Path(filename);
            return hdfs.exists(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    public static boolean mkdir(FileSystem hdfs, String dirPath) {
        if (checkFileExist(hdfs, dirPath))
            return true;
        try {
            Path path = new Path(dirPath);
            return hdfs.mkdirs(path);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    public static boolean mkfile(FileSystem hdfs, String filePath) {
        try {
            Path path = new Path(filePath);
            FSDataOutputStream fsos = hdfs.create(path, false);
            fsos.close();
            return true;
        } catch (IllegalArgumentException | IOException e) {
            e.printStackTrace();
        }
        return false;
    }
    public static void delete(FileSystem hdfs, String src) {
        try {
            Path path = new Path(src);
            if (hdfs.isDirectory(path)) {
                hdfs.delete(path, true);
            } else if (hdfs.isFile(path)) {
                hdfs.delete(path, false);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static boolean putString(FileSystem hdfs, String context, String hdfsPath, boolean overwrite) {
        try {
            boolean isFileExist = checkFileExist(hdfs, hdfsPath);
            if(!checkFileExist(hdfs, hdfsPath.substring(0, hdfsPath.lastIndexOf("/")))
                    || !overwrite && isFileExist){
                return false;
            }else if(isFileExist) {
                delete(hdfs, hdfsPath);
            }
            FSDataOutputStream fsos = hdfs.create(new Path(hdfsPath), true);
            byte[] buffer = context.getBytes();
            fsos.write(buffer);
            fsos.close();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    public static String getString(FileSystem hdfs, String hdfsPath) {
        try {
            Path path = new Path(hdfsPath);
            FSDataInputStream fdis = hdfs.open(path);
            StringBuffer stringBuffer = new StringBuffer();
            int num;
            while ((num = fdis.read()) != -1) {
                stringBuffer.append((char)num);
            }
            fdis.close();
            return new String(stringBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
    public static void main(String[] args) throws Exception{
        String filename = "/data/test/tt1";
        FileSystem fileSystem = MyHdfsUtils.getFileSystem();
        MyHdfsUtils.putString(fileSystem, "123", filename, true);
        String s = MyHdfsUtils.getString(fileSystem, filename);
        MyHdfsUtils.delete(fileSystem, filename);
        System.out.println(s);
    }
}