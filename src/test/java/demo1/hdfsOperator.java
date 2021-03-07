package demo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class hdfsOperator {
    @Test
    public void getFilesystem1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop11:8020");
        //使用get方法获取hdfs的文件系统，使用一个参数
        FileSystem fs = FileSystem.get(configuration);
        System.out.println(fs.toString());
        fs.close();
    }
    @Test
    public  void getFilesystem2() throws IOException {
        Configuration configuration = new Configuration();
        //使用gey方法获取hdfs的文件系统，使用两个参数
        FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop11:820"), configuration);
        System.out.println(fs.toString());
        fs.close();
    }
    @Test
    public void getFileSystem3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://h" +
                "adoop11:8020");
        //使用instance方法获取文件系统，使用一个参数
        FileSystem fs = FileSystem.newInstance(configuration);
        System.out.println(fs.toString());
        fs.close();
    }
    @Test
    public  void getFilesystem4() throws IOException {
        Configuration configuration = new Configuration();
        //使用instance方法获取文件系统，使用两个参数
        FileSystem fs = FileSystem.newInstance(URI.create("hdfs://hadoop11:8020"), configuration);
        System.out.println(fs.toString());
        fs.close();
    }

    @Test
    //遍历hdfs指定的目录下的所有文件
    public void listFile2() throws IOException {
        FileSystem fs = FileSystem.newInstance(URI.create("hdfs://hadoop11:8020"), new Configuration());
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("hdfs://hadoop11:8020/day03"), true);
        while(remoteIterator.hasNext()){
            System.out.println(remoteIterator.next().getPath());
        }
        fs.close();
    }

    //下载文件到本地
    //方法1：使用输入输出流，不推荐使用
    @Test
    public void downFileTolocal() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop11:8020"), new Configuration());
        FSDataInputStream inputStream = fileSystem.open(new Path("hdfs://hadoop11:8020/aa.txt"));
        FileOutputStream fileOutputStream = new FileOutputStream(new File("E:\\aa.txt"));
        IOUtils.copy(inputStream,fileOutputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(fileOutputStream);
        fileOutputStream.close();
    }

    //方法2：（推荐使用）
    @Test
    public void downFileTolocal2() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://hadoop11:8020"), new Configuration());
        fileSystem.copyToLocalFile(new Path("hdfs://hadoop11:8020/aa.txt"),new Path("E:\\abc.txt"));
        fileSystem.close();
    }

    @Test
    //创建文件
    public void createDir() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop11:8020"), new Configuration());
        fileSystem.mkdirs(new Path("hdfs://hadoop11:8020/day03/day03"));
        fileSystem.close();
    }



    //上传文件
    @Test
    public void uploadFile() throws Exception {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop11:8020"), new Configuration());

        fileSystem.copyFromLocalFile(new Path("E:\\01.txt"),
                new Path("hdfs://hadoop11:8020/day03"));

        fileSystem.close();


    }


    //hadoop的权限问题
    /*
    1,停止hdfs集群
    start-dfs.sh
    2,修改hdfs-site.xml文件
    <property>
                <name>dfs.permissions</name>
                <value>true</value>
        </property>
    3，同步其他两个参数信息
    scp hdfs-site.xml hadoop12:$PWD
    scp hdfs-site.xml hadoop12:$PWD
    4,start-dfs.sh
    5,修改目录权限，重新上传，此时报错，
    6，伪装用户
     */
    @Test
    public void uploadFile1() throws Exception {
        //在后面添加一个用户的参数
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop11:8020"), new Configuration(),"root");

        fileSystem.copyFromLocalFile(new Path("E:\\01.txt"),
                new Path("hdfs://hadoop11:8020/day03"));

        fileSystem.close();


    }
    @Test
    public void mergeFile() throws Exception {
        //获取分布式的文件系统
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop11:8020"), new Configuration(),"root");
        //获取hdfs文件上面的输出流
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("hdfs://hadoop11:8020/bigfile.xml"));
        //通过本地文件系统，遍历小文件，将每一个小文件读成一个输入流
        LocalFileSystem localFileSystem = FileSystem.getLocal(new Configuration());
        //通过本地文件系统，遍历本地所有的小文件
        FileStatus[] fileStatuses = localFileSystem.listStatus(new Path("file:///E:\\test"));
        //遍历小文件,得到文件path，使用本地文件系统的实例的open的方法，将path转为输入流
        for (FileStatus fileStatus:fileStatuses){
            Path path = fileStatus.getPath();
            FSDataInputStream dataInputStream = localFileSystem.open(path);
            IOUtils.copy(dataInputStream,fsDataOutputStream);
            IOUtils.closeQuietly(dataInputStream);
        }
        fsDataOutputStream.close();
        localFileSystem.close();
        fileSystem.close();
    }
}
