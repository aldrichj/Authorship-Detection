import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if(args.length != 4){
            System.out.printf("Usage: ProcessLogs <Train input dir>, <Unknown input dir>, <output dir>, <unknown author name>\n");
            System.exit(-1);
        }


        String unknown_path = args[0];
        String train_path   = args[1];
        String prefix = args[2];
        String unknownTag = args[3];




        //run with train
        run(train_path,prefix,"/train",unknownTag);

        // run with unknown
        run(unknown_path,prefix,"/unknown",unknownTag);



        //merge and compute

        /**
         * Merge and compute the full TF*IDF
         * Since the unknown IDF is -1.0 we need to find the corresponding word in the known text to assign it to.
         */


        Path train = new Path(prefix+"/train/tf");
        Path unknown = new Path(prefix+"/unknown/tf");

        Configuration conf3 = new Configuration();
        conf3.set("uAuthor",unknownTag.trim());
        Job job3 = new Job(conf3,"Merge");


        MultipleInputs.addInputPath(job3,
                unknown,
                TextInputFormat.class,
                MergeMapper.class);

        MultipleInputs.addInputPath(job3,
                train,
                TextInputFormat.class,
                MergeMapper.class);

        job3.setReducerClass(MergeReducer.class);

        FileOutputFormat.setOutputPath(job3,new Path(prefix+"/tfidf/"));

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setJarByClass(Main.class);
        boolean mergeSuccess = job3.waitForCompletion(true);

        if(!mergeSuccess)
            System.exit(-1);


        /**
         * Sums the numerator
         * Calculates the author section of the denominator including the unknown author
         */
        Configuration conf4 = new Configuration();
        conf4.set("uAuthor",unknownTag.trim());
        Job job4 = new Job(conf4,"Count Based on Author");
        job4.setJarByClass(Main.class);
        FileInputFormat.setInputPaths(job4, new Path(prefix+"/tfidf/"));
        FileOutputFormat.setOutputPath(job4, new Path(prefix+"/calc/"));

        job4.setMapperClass(CalcMapper.class);
        job4.setReducerClass(CalcReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);


        boolean calcSuccess = job4.waitForCompletion(true);


        if(!calcSuccess)
            System.exit(-1);


        /**
         * Multiply A (Denominator) into B (Denominator)
         * Then divide numerator by denominator creating our cosine for that author
         */

        Configuration conf5 = new Configuration();
        conf5.set("uAuthor",unknownTag.trim());
        Job job5 = new Job(conf5,"Cosine");
        job5.setJarByClass(Main.class);
        FileInputFormat.setInputPaths(job5, new Path(prefix+"/calc/"));
        FileOutputFormat.setOutputPath(job5, new Path(prefix+"/cosine/"));

        job5.setMapperClass(CosineMapper.class);
        job5.setReducerClass(CosineReducer.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);


        boolean cosSuccess = job5.waitForCompletion(true);


        if(!cosSuccess)
            System.exit(-1);





        /**
         * Sorts top ten authors based on the cosine value
         */

        Configuration conf6 = new Configuration();
        conf6.set("uAuthor",unknownTag);
        Job job6 = new Job(conf6,"Top Ten");
        FileInputFormat.setInputPaths(job6,new Path(prefix+"/cosine/"));
        FileOutputFormat.setOutputPath(job6,new Path(prefix+"/topTen/"));
        job6.setMapperClass(TopTenMapper.class);
        job6.setCombinerClass(TopTenReducer.class);
        job6.setReducerClass(TopTenReducer.class);
        job6.setOutputKeyClass(NullWritable.class);
        job6.setOutputValueClass(Text.class);
        job6.setJarByClass(Main.class);
        boolean topSuccess = job6.waitForCompletion(true);


        if(!topSuccess)
            System.exit(-1);





    }

    protected static void run(String input, String prefix, String path, String unknownTag) throws IOException, ClassNotFoundException, InterruptedException {

        String data = prefix+path+"/data";
        String max = prefix+path+"/max";
        String tf = prefix+path+"/tf";



        /**
         * Counts words based on author
         *
         */
        Configuration conf = new Configuration();
        Job job = new Job(conf,"Count Based on Author");
        job.setJarByClass(Main.class);
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(data));

        job.setMapperClass(NGramMapper.class);
        job.setReducerClass(NGramReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        boolean nGramSuccess = job.waitForCompletion(true);


        if(!nGramSuccess)
            System.exit(-1);


        /**
         * Calculates the max occurring word per author
         *
         */
        Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1,"Max Occurring");
        job1.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job1, new Path(data));
        FileOutputFormat.setOutputPath(job1, new Path(max));

        job1.setMapperClass(MaxWordMapper.class);
        job1.setReducerClass(MaxWordReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        boolean maxSuccess = job1.waitForCompletion(true);

        Counters counters = job1.getCounters();
        long count = counters.findCounter("org.apache.hadoop.mapred.Task$Counter","REDUCE_INPUT_GROUPS").getValue();

        if(!maxSuccess)
            System.exit(-1);


        /**
         * Finds the unique occurrences of a word through all the authors
         * then calculates the TF*IDF for every word.
         */

        Configuration conf2 = new Configuration();
        conf2.set("AuthTotal",Long.toString(count));
        conf2.set("unknownAuth",unknownTag);
        Job job2 = new Job(conf2,"TFIDF");

        job2.setJarByClass(Main.class);

        FileInputFormat.setInputPaths(job2, new Path(max));
        FileOutputFormat.setOutputPath(job2, new Path(tf));

        job2.setMapperClass(CorpMapper.class);
        job2.setReducerClass(CorpReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        boolean tfSuccess = job2.waitForCompletion(true);


        if(!tfSuccess)
            System.exit(-1);


    }
}
