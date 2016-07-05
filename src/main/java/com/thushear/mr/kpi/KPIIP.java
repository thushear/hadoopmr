package com.thushear.mr.kpi;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Created by kongming on 2016/7/5.
 */
public class KPIIP {


    public static void main(String[] args) throws IOException {
        String input = "hdfs://master:9000/user/hadoop/log_kpi";
        String output = "hdfs://master:9000/user/hadoop/log_kpi/ip";

        JobConf conf = new JobConf(KPIIP.class);
        conf.setJobName("KPIIP");
        conf.addResource("classpath:/hadoop/core-site.xml");
        conf.addResource("classpath:/hadoop/hdfs-site.xml");
        conf.addResource("classpath:/hadoop/mapred-site.xml");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapperClass(KPIIPMapper.class);
        conf.setCombinerClass(KPIIPReducer.class);
        conf.setReducerClass(KPIIPReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(input));
        FileOutputFormat.setOutputPath(conf, new Path(output));

        JobClient.runJob(conf);

        System.exit(0);
    }


    public static class KPIIPReducer extends MapReduceBase implements Reducer<Text,Text,Text,Text>{

        private Text result = new Text();

        private Set<String> count = new HashSet<String>();

        public void reduce(Text text, Iterator<Text> iterator, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            while (iterator.hasNext()){
                count.add(iterator.next().toString());
            }
            result.set(String.valueOf(count.size()));
            outputCollector.collect(text,result);
        }
    }


    public static class KPIIPMapper extends MapReduceBase implements Mapper<Object,Text,Text,Text> {

        private Text word = new Text();

        private Text ips = new Text();

        public void map(Object o, Text text, OutputCollector<Text, Text> outputCollector, Reporter reporter) throws IOException {
            KPI kpi = KPI.filterIPs(text.toString());
            if (kpi.isValid()){
                word.set(kpi.getRequest());
                ips.set(kpi.getRemote_addr());
                outputCollector.collect(word,ips);
            }
        }
    }


}
