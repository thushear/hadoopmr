package com.thushear.mr.kpi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by kongming on 2016/7/5.
 */
public class KPIPV {


    public static void main(String[] args) throws IOException {
        String input = "hdfs://master:9000/user/hadoop/log_kpi";
        String output = "hdfs://master:9000/user/hadoop/log_kpi/pv";

        JobConf conf = new JobConf(KPIPV.class);
        conf.setJobName("KPIPV");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(IntWritable.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        conf.setMapperClass(KPIPVMapper.class);
        conf.setCombinerClass(KPIPVReducer.class);
        conf.setReducerClass(KPIPVReducer.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);
        System.exit(0);

    }



    public static class KPIPVReducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        private IntWritable result = new IntWritable();

        public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()){
                sum += iterator.next().get();
           }
            result.set(sum);
            outputCollector.collect(text,result);
        }
    }


    public static class KPIPVMapper extends MapReduceBase implements Mapper<Object,Text,Text,IntWritable> {

        private IntWritable one = new IntWritable(1);

        private Text word = new Text();

        public void map(Object o, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterPVs(text.toString());
            if (kpi.isValid()){
                word.set(kpi.getRequest());
                outputCollector.collect(word,one);
            }
        }
    }







}
