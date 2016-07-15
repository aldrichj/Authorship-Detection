import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by jeremy on 3/24/16.
 */
public class TopTenReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

    @Override
    public void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        TreeMap<Double,String> top = new TreeMap<>();

        for(Text value: values){


            String[] tokens = value.toString().split("#");

            String author = tokens[0];
            double cosine = Double.parseDouble(tokens[1]);

            top.put(cosine, author);

            if(top.size() > 10){
                top.remove(top.firstKey());
            }
        }

        TreeMap<Double,String> revTop = new TreeMap<>(Collections.reverseOrder());
        revTop.putAll(top);
        // Output our ten records to the reducers with a null key
        for(Map.Entry<Double,String> entry : revTop.entrySet()) {
            double cos = entry.getKey();
            String value = entry.getValue();

            context.write(NullWritable.get(), new Text(value+"#"+Double.toString(cos)));
        }




    }
}
