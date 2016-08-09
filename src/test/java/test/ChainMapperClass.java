package test;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by root on 2016/7/29.
 */
public class ChainMapperClass {
    public static class TokenizerMapper extends
            Mapper<Object, Text, TextPair, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private static TextPair textPair = new TextPair();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                Text word2 = new Text();
                word2.set("mapper1");
                Text word3 = new Text();
                word3.set("ggg");
                textPair.set(word, word2, word3);
                context.write(textPair, one);
            }
        }
    }

    public static class TokenizerMapper2 extends
            Mapper<TextPair, IntWritable, TextPair, IntWritable> {

        private static TextPair textPair = new TextPair();

        public void map(TextPair key, IntWritable value, Context context)
                throws IOException, InterruptedException {

            Text text3 = new Text();
            text3.set("fff");
            textPair.set(key.getFirst(), key.getSecond(), text3);
            context.write(textPair, value);

        }

    }

    public static class IntSumReducer extends
            Reducer<TextPair, IntWritable, TextPair, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(TextPair key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");

        // //////////////Here add chaining code for mappers
        Configuration mapAConf = new Configuration(false);
        ChainMapper.addMapper(job, TokenizerMapper.class, Object.class,
                Text.class, TextPair.class, IntWritable.class, mapAConf);

        Configuration mapBConf = new Configuration(false);
        ChainMapper.addMapper(job, TokenizerMapper2.class, TextPair.class,
                IntWritable.class, TextPair.class, IntWritable.class, mapBConf);
        // //////////////end of chaining for mappers
        job.setJarByClass(ChainMapperClass.class);

        // //job.setMapperClass(TokenizerMapper.class);

        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // //job.setMapOutputKeyClass(TextPair.class);
        // //job.setMapOutputValueClass(IntWritable.class);

        // job.setOutputKeyClass(TextPair.class);
        // job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("d:\\test\\chain\\test.txt"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\test\\chain\\out"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
