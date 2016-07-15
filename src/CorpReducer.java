import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by aldrichj on 3/31/16.
 */
public class CorpReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        String numAuthors = conf.get("AuthTotal");
        String unknownAuth = conf.get("unknownAuth");



        ArrayList<String> data = new ArrayList<String>();
        Map<String,String> authors = new HashMap<String,String>();
        int uniqueCount = 0;

        double TF = 0.0;
        // (word),(author,count,max)
        for(Text value: values){
            //System.out.println(value.toString());
            String[] line = value.toString().split("#");
            String author = line[0];
            String count = line[1];
            String max = line[2];

            if(!authors.containsKey(author.trim())){
                authors.put(author.trim(),"1");
                uniqueCount++;
            }

            TF = Double.parseDouble(count) / Double.parseDouble(max);

            data.add(author+"#"+TF);


        }

        for (String entry: data){

            String[] items = entry.split("#");
            String author = items[0];
            double tf = Double.parseDouble(items[1]);
            double IDF = -1.0;

            if(!unknownAuth.equals(author.trim())) {
                IDF  = (Math.log(Double.parseDouble(numAuthors) / uniqueCount))/Math.log(2);

            }


            //System.out.println(tf+" "+numAuthors+" "+authorUnique+" "+IDF);

            // (word,author),(TF,IDF)
            context.write(new Text(key+"#"+author),new Text("#"+Double.toString(tf)+"#"+Double.toString(IDF)));

        }



    }
}
