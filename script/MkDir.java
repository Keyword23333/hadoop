package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MkDir {
    @Test
    public void testmkdir() throws IOException,InterruptedException, URISyntaxException {
        URI uri = new URI("hdfs://hadoop081:9000");
        Configuration configuration = new Configuration();
        String user = "hadooptest1";
        FileSystem fs = FileSystem.get(uri, configuration, user);
        fs.mkdirs(new Path("/input"));
        fs.close();
    }
}
