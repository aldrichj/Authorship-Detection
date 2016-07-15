import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by aldrichj on 3/31/16.
 */
public class NGramMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // split author
        String[] tokens = value.toString().split("<===>");

        String author = tokens[0].trim();
        String line = tokens[1].trim().toLowerCase();



        String[] words = line.split("\\s+");

        for (int i = 0; i < words.length ; i++) {
            String parsedWord = words[i].replaceAll("[^a-zA-Z ]","");
            if(parsedWord.trim().length() > 0){

                // (word,author),(count)
                context.write(new Text(parsedWord+"#"+author),new Text("1"));

            }


        }




    }



}
