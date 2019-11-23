package com.samhad;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ConvertToSequenceFile extends Configured implements Tool {

    public static void main(String[] args) {

        try {
            int res = ToolRunner.run(new Configuration(), new ConvertToSequenceFile(), args);
            System.exit(res);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public int run(String[] args) throws Exception {

        String[] otherArgs = new GenericOptionsParser(this.getConf(), args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: hadoop jar SequenceFileExample.jar </input-path> </output-path>");
            return 2;
        }

        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inputFile = new Path(args[0]);
        Path outputFile = new Path(args[1]);
        Boolean isCompressionNeeded = Boolean.valueOf(args[2]);
        FSDataInputStream inputStream;
        LongWritable key = new LongWritable();
        Text value = new Text();
        SequenceFile.Writer writer = null;

        if (isCompressionNeeded) {
            writer = SequenceFile.createWriter(fs, conf,
                    outputFile, key.getClass(), value.getClass(), SequenceFile.CompressionType.BLOCK, new GzipCodec());
        } else {
            writer = SequenceFile.createWriter(fs, conf, outputFile, key.getClass(), value.getClass());
        }

        FileStatus[] fileStatuses = fs.listStatus(inputFile);
        for (FileStatus fst : fileStatuses) {

            System.out.println("Processing file: " + fst.getPath().getName());
            inputStream = fs.open(fst.getPath());
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));

            int noOfLines = 0;

            while (inputStream.available() > 0) {

                value.set(br.readLine());
                writer.append(new LongWritable(value.getLength()), value);
                noOfLines++;
            }

            System.out.println("Number of lines written " + noOfLines);
        }

        fs.close();
        IOUtils.closeStream(writer);
        System.out.println("Sequence File Created Successfully");

        return 0;
    }
}
