package MR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//定义四个泛型
/*
k1--行偏移量
v1--行内容
k2--单词内容
v2--单词的次数
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {
    /**
     *必须重写map方法，实现逻辑
     * @param key       LongWritable key--代表k1
     * @param value     Text value--代表v1
     * @param context   Context context--上下文对象，将数据往下发送
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      //第一步：将行数据进行切分,用逗号进行分割
      String[] words = value.toString().split(",");
      //遍历切分的文字，通过write方法向下发送
      for(String word:words){
          context.write(new Text(word),new IntWritable(1));
      }
    }
}
