package example;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class UpLoad {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        URI uri = new URI("hdfs://hadoop081:9000");
        Configuration conf = new Configuration();
        String user = "hadooptest1";
        Path srcPath = new Path("E:/Documents/GitHub/hadoop/minset");//本地路径
        Path dstPath = new Path("/input");// 这个是你hdfs下的路径
        FileSystem fs = FileSystem.get(uri, conf, user);
        fs.copyFromLocalFile(srcPath,dstPath);//上传文件
        String filename = "hdfs://hadoop081:9000/input/minset/file0.txt"; //检测文件是否存在
        if (fs.exists(new Path(filename))) {
            System.out.println("文件存在");
        } else {
            System.out.println("文件不存在");
        }
        FSDataInputStream out = fs.open(new Path(filename)); // 打开上传的文件 并且输出里面的内容
        IOUtils.copyBytes(out,System.out,1024,true);
    }

}
