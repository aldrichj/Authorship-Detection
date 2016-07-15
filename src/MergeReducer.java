import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by jeremy on 4/1/16.
 */
public class MergeReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {

        // brings in unknown author tag
        Configuration conf = context.getConfiguration();
        String unknowAuth = conf.get("uAuthor");

        // if a word exists in any authors writings
        boolean train_located = false;
        // if a word exists in the unknown authors writings
        boolean unknow_located = false;
        // array of data
        ArrayList<String> data = new ArrayList<>();
        //Unknown TFIDF
        double unknow_TFIDF = 0.0;
        double train_TFIDF = 0.0;

        double IDF = 0.0;

        // (word),(author,TF,IDF)
        for( Text value: values){
            String[] line = value.toString().split("#");

            String author = line[0];
            String TF = line[1];




            if(!train_located && !unknowAuth.equals(author.trim())){
                String trainIDF = line[2];
                IDF = Double.parseDouble(trainIDF);
                train_located = true;
            }else if(!unknow_located && unknowAuth.equals(author.trim())){
                if(train_located) {
                    unknow_TFIDF = Double.parseDouble(TF) * IDF;
                    //System.out.println(key+" "+TF+" "+IDF+" "+unknow_TFIDF);
                    unknow_located = true;
                }

            }


            data.add(author+"#"+TF);

        }

        if(train_located && unknow_located)
            for(String entry: data){

                String[] line = entry.split("#");
                String author = line[0];
                double TF = Double.parseDouble(line[1]);

                double TFIDF = TF * IDF;


                /**
                 * Removes words that are used a lot by every author.
                 * Which gives the word a weight of 0. So to reduce the amount of work going forward
                 * we might as well remove all of these.
                 */

                if(TFIDF > 0.0){
                    double numer = TFIDF * unknow_TFIDF;

                    // (word,author),(TFIDF)
                    context.write(new Text(key+"#"+author), new Text("#"+Double.toString(TFIDF)+"#"+numer));
                }

            }



    }
}
