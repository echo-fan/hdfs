package MR;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
//reduce的泛型参数代表k2，v2，k3，v3
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    /**
     *和map类同理，必须重写reduce方法
     * @param key   Text key--k2
     * @param values  Iterable<IntWritable> values--是一个集合，v2的类型
     * @param context   承上启下，向下发送
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
       //sum用来统计values的个数
        int sum=0;
        for (IntWritable value:values){

       // ntWritable是一个对象，而如果想要用int参加计算，那么我们需要调用IntWritable对象的一个成员get()，这个函数返回int.
        sum+=value.get();
    }
        context.write(key,new IntWritable(sum));
    }
}
