package com.mkwhitacre.spark.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class CrunchSparkWordCount extends Configured implements Tool, Serializable{

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        ToolRunner.run(config, new CrunchSparkWordCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        String[] jars = JavaSparkContext.jarOfClass(CrunchSparkWordCount.class);
        for(String jarFile : jars){
            sc.addJar(jarFile);
        }

        SparkPipeline pipeline = new SparkPipeline(sc, "Crunch Spark Count");

        String inputFile = args[0];
        final int threshold = Integer.parseInt(args[1]);

        PCollection<String> read = pipeline.read(From.textFile(new Path(inputFile)));

        PTable<String, String> keyedStrings = read.parallelDo(new DoFn<String, Pair<String, String>>() {
            @Override
            public void process(String s, Emitter<Pair<String, String>> emitter) {
                for(String value: s.split(" ")){
                    emitter.emit(Pair.of(value, value));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));

        PCollection<String> filteredStrings = keyedStrings.groupByKey().parallelDo(new DoFn<Pair<String, Iterable<String>>, String>() {
            @Override
            public void process(Pair<String, Iterable<String>> stringIterablePair, Emitter<String> emitter) {
                int count = 0;
                for (String value : stringIterablePair.second()) {
                    count++;
                }
                if(count >= threshold){
                    emitter.emit(stringIterablePair.first());
                }
            }
        }, Writables.strings());

        PCollection<Pair<String, Integer>> pairPCollection = filteredStrings.parallelDo(new DoFn<String, Pair<String, Integer>>() {
            @Override
            public void process(String s, Emitter<Pair<String, Integer>> emitter) {
                for (char c : s.toCharArray()) {
                    emitter.emit(Pair.of("" + c, 1));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.ints())).groupByKey().combineValues(Aggregators.SUM_INTS());

        for(Pair<String, Integer> value: pairPCollection.materialize()){
            System.out.println("("+value.first()+", "+value.second()+")");
        }

        pipeline.done();

        return 0;
    }
}
