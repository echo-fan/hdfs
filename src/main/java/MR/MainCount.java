package MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MainCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        //这里执行完成后，得到一个int类型的返回值，表示我们的程序退出状态码
        //如果退出的是0，程序执行成功
        //这里设置configuration，相当于我们把父类的configuration设置值了
        int run = ToolRunner.run(new Configuration(), new MainCount(), args);
        System.exit(run);
    }
    /*
    必须实现run方法，这里面就是通过job对象来组装我们程序，组装八个类
     */
    @Override
    public int run(String[] strings) throws Exception {
        //第一步：读取文件，解析成key，value

        //获取一个job，job的作用主要是用来组装MapReduce各个阶段的任务
        //Job的getInstance方法需要两个参数，第一个是一个Configuration的实例，第二个是jobName，可以自己随便定义
        Job job = Job.getInstance(super.getConf(), "xxx");
        //把job和需要操作的文件所在的目录添加进来，注意此时增加的目录
        TextInputFormat.addInputPath(job,new Path("hdfs://hadoop11:8020/wordcount"));

        //打包到集群，必须添加如下配置，不添加的话会报错
        job.setJarByClass(MainCount.class);

        //指定job需要执行的任务的原始数据的类型
        job.setInputFormatClass(TextInputFormat.class);

        //第二步：自定义map类
        //指定map
        job.setMapperClass(WordCountMapper.class);
        //设置k2，v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


        //第七步：自定义reduce类
        //指定reduce
        job.setReducerClass(WordCountReducer.class);
        //设置k3.v3
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //第八步：输出文件
        //指定路径
        TextOutputFormat.setOutputPath(job,new Path("hdfs://hadoop11:8020/wordcountoutput"));

        //设置job输出数据的类型
        job.setOutputFormatClass(TextOutputFormat.class);

        //提交任务到集群
        boolean b = job.waitForCompletion(true);
        //使用三元表达确认返回值
        return b?0:1;
    }
}
