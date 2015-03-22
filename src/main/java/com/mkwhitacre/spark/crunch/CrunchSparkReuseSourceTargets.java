package com.mkwhitacre.spark.crunch;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.MapFn;
import org.apache.crunch.PCollection;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.PTable;
import org.apache.crunch.Pair;
import org.apache.crunch.Target.WriteMode;
import org.apache.crunch.fn.Aggregators;
import org.apache.crunch.impl.spark.SparkPipeline;
import org.apache.crunch.io.From;
import org.apache.crunch.io.To;
import org.apache.crunch.types.writable.Writables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class CrunchSparkReuseSourceTargets extends Configured implements Tool, Serializable{

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        ToolRunner.run(config, new CrunchSparkReuseSourceTargets(), args);
    }

    @Override
    public int run(String[] args) throws Exception {

        JavaSparkContext sc = new JavaSparkContext(new SparkConf());
        String[] jars = JavaSparkContext.jarOfClass(CrunchSparkReuseSourceTargets.class);
        for(String jarFile : jars){
            sc.addJar(jarFile);
        }

        SparkPipeline pipeline = new SparkPipeline(sc, "Crunch Spark Count");

        String inputFile = args[0];
        final int threshold = Integer.parseInt(args[1]);

        PCollection<String> read = pipeline.read(From.textFile(new Path(inputFile)));

        PTable<String, String> keyedStrings = read.parallelDo("Split input into individual words",new DoFn<String, Pair<String, String>>() {
            @Override
            public void process(String s, Emitter<Pair<String, String>> emitter) {
                for(String value: s.split(" ")){
                    emitter.emit(Pair.of(value, value));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.strings()));

        PGroupedTable<String, String> groupedStrings = keyedStrings.groupByKey();
        
        PTable<String, Integer> wordCount = groupedStrings.parallelDo("Calculate Word Count",new MapFn<Pair<String, Iterable<String>>, Pair<String, Integer>>(){

			@Override
			public Pair<String, Integer> map(Pair<String, Iterable<String>> input) {
				int count = 0;
				for(String s: input.second()){
					count++;
				}
				return Pair.of(input.first(), count);
			}
        }, Writables.tableOf(Writables.strings(),  Writables.ints()));
        wordCount.write(To.textFile("/tmp/word_count_out"), WriteMode.OVERWRITE);
        		
        		
        PCollection<String> filteredStrings  = wordCount.parallelDo("Filter words not satisfying threshold",new DoFn<Pair<String, Integer>, String>() {
            @Override
            public void process(Pair<String, Integer> stringIterablePair, Emitter<String> emitter) {
                if(stringIterablePair.second() >= threshold){
                    emitter.emit(stringIterablePair.first());
                }
            }
        }, Writables.strings());

        PCollection<Pair<String, Integer>> charCountPCollection = filteredStrings.parallelDo("Convert strings to Pair<char, int>",new DoFn<String, Pair<String, Integer>>() {
            @Override
            public void process(String s, Emitter<Pair<String, Integer>> emitter) {
                for (char c : s.toCharArray()) {
                    emitter.emit(Pair.of("" + c, 1));
                }
            }
        }, Writables.tableOf(Writables.strings(), Writables.ints())).groupByKey().combineValues(Aggregators.SUM_INTS());
        
        charCountPCollection.write(To.textFile("/tmp/char_count_out"), WriteMode.OVERWRITE);
        
        for(Pair<String, Integer> value: charCountPCollection.materialize()){
            System.out.println("("+value.first()+", "+value.second()+")");
        }

        pipeline.done();

        return 0;
    }
}
