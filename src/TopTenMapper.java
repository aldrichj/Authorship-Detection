import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;

/**
 * Created by jeremy on 3/24/16.
 */
public class TopTenMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private    TreeMap<Double,String> top = new TreeMap<>();
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] tokens = value.toString().split("#");
        String author = tokens[0];
        double cosine = Double.parseDouble(tokens[1]);


        top.put(cosine,author);


        if(top.size() > 10){
            top.remove(top.firstKey());
        }



        // (author),(cosine)


    }

    protected void cleanup( Context context)
    throws IOException, InterruptedException {

        TreeMap<Double,String> revTop = new TreeMap<>(Collections.reverseOrder());
        revTop.putAll(top);
        // Output our ten records to the reducers with a null key
        for(Map.Entry<Double,String> entry : revTop.entrySet()) {

            double key = entry.getKey();
            String value = entry.getValue();

            context.write(NullWritable.get(), new Text(value+"#"+Double.toString(key)));
        }
    }
}

