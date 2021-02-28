import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;




public class sDBN extends Configured implements Tool {
    public static Path urlspath = new Path("/user/vakulenko.yulia/data/url.data");
    public static Path qidspath = new Path("/user/vakulenko.yulia/data/queries.tsv");


    public static class sDBNMapper extends Mapper<LongWritable, Text, Text, Text> {
        public static Map<String, String> url_id = new HashMap<>();
        public static Map<String, String> id_url = new HashMap<>();
        public static Map<String, String> t_qid = new HashMap<>();
        public static Map<String, String> qid_t = new HashMap<>();


        public static void url_extractor()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(urlspath);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        String okurl = args[1].charAt(args[1].length()-1)=='/' ? args[1].substring(0, args[1].length()-1) : args[1];
                        url_id.put(okurl,id);
                        id_url.put(id, okurl);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        public static void query_extractor()
        {
            try {
                FileSystem fs = FileSystem.get(new Configuration());
                FileStatus[] statuses = fs.globStatus(qidspath);
                for (FileStatus status : statuses)
                {
                    BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status.getPath()), StandardCharsets.UTF_8));
                    String line;
                    line=br.readLine();
                    while (line != null){
                        String[] args = line.split("\t");
                        String id = args[0];
                        t_qid.put(args[1],id);
                        qid_t.put(id, args[1]);
                        line=br.readLine();
                    }
                }
            }
            catch (Exception ignored){}
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            url_extractor();
            query_extractor();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException{


            String[] line = value.toString().split("\t");
            String[] ind = line[0].split("@");
            String nid;
            HashSet<String> clset = new HashSet<>();
            nid = t_qid.get(ind[0]);
            if (nid == null) {
                return;
            }
            String[] ts = line[3].split(",");
            String[] rawlinks = line[1].split(",");
            String[] rawclicks = line[2].split(",");
            StringBuilder lb = new StringBuilder();
            int off = 0;
            while (off < rawlinks.length - 1) {
                int noff = 1;
                if (rawlinks[off + noff].startsWith("http://")) {
                    lb.append(rawlinks[off]);
                    lb.append("\t");
                    off++;
                } else {
                    int trash = 0;
                    rawlinks[off] += "," + rawlinks[off + noff];
                    while (off + noff + 1 < rawlinks.length) {
                        if (rawlinks[off].length() > 7) {
                            if (url_id.get(rawlinks[off].substring(7)) != null) {
                                break;
                            }
                        }
                        noff++;
                        if (rawlinks[off + noff].startsWith("http://")) {
                            trash = 1;
                            break;
                        }
                        rawlinks[off] += "," + rawlinks[off + noff];

                    }
                    if (trash == 0 && off + noff + 1 != rawlinks.length) {
                        lb.append(rawlinks[off]);
                        lb.append("\t");
                        off += noff;
                    } else {
                        String[] trashedtokens = rawlinks[off].split(",");
                        int cutborder = 0;
                        if (off + noff + 1 >= rawlinks.length) {
                            cutborder = 1;
                        }
                        for (String token : trashedtokens) {
                            if (cutborder == 1 && token.equals(trashedtokens[trashedtokens.length - 1])) {
                                continue;
                            }
                            lb.append(token);
                            lb.append("\t");
                        }
                        off += noff;
                    }
                }
            }
            lb.append(rawlinks[rawlinks.length - 1]);
            String[] links = lb.toString().split("\t");


            StringBuilder cb = new StringBuilder();
            off = 0;
            while (off < rawclicks.length - 1) {
                int noff = 1;
                if (rawclicks[off + noff].startsWith("http://")) {
                    cb.append(rawclicks[off]);
                    cb.append("\t");
                    off++;
                } else {
                    int trash = 0;
                    rawclicks[off] += "," + rawclicks[off + noff];
                    while (off + noff + 1 < rawclicks.length) {
                        if (rawclicks[off].length() > 7) {
                            if (url_id.get(rawclicks[off].substring(7)) != null) {
                                break;
                            }
                        }
                        noff++;
                        if (rawclicks[off + noff].startsWith("http://")) {
                            trash = 1;
                            break;
                        }
                        rawclicks[off] += "," + rawclicks[off + noff];
                    }
                    if (trash == 0 && off + noff + 1 != rawclicks.length) {
                        cb.append(rawclicks[off]);
                        cb.append("\t");
                        off += noff;
                    } else {
                        String[] trashedtokens = rawclicks[off].split(",");
                        int cutborder = 0;
                        if (off + noff + 1 >= rawclicks.length) {
                            cutborder = 1;
                        }
                        for (String token : trashedtokens) {
                            if (cutborder == 1 && token.equals(trashedtokens[trashedtokens.length - 1])) {
                                continue;
                            }
                            cb.append(token);
                            cb.append("\t");
                        }
                        off += noff;
                    }
                }
            }
            cb.append(rawclicks[rawclicks.length - 1]);
            String[] clicks = cb.toString().split("\t");
            for (int i = 0; i < links.length; i++) {
                if (links[i].length() > 9) {
                    links[i] = links[i].substring(7);
                }
            }
            for (int i = 0; i < clicks.length; i++) {
                if (clicks[i].length() > 9) {
                    clicks[i] = clicks[i].substring(7);
                    clset.add(clicks[i]);
                }
            }
            for (String link : links) {
                if (url_id.containsKey(link)) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(nid).append("\t");
                    if (clset.contains(link)) {
                        if (link.equals(clicks[clicks.length - 1])) {
                            sb.append("1\t1");
                        } else {
                            sb.append("0\t1");
                        }
                    } else {
                        sb.append("0\t0");
                    }
                    context.write(new Text(url_id.get(link)), new Text(sb.toString()));
                }
            }
        }
    }

    public static class sDBNReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int s_d = 0;
            int s_n = 0;
            int a_d = 0;
            int a_n = 0;
            for(Text value : values){
                String[] line = value.toString().split("\t");
                s_n += Integer.parseInt(line[1]);
                a_n += Integer.parseInt(line[2]);
                s_d += Integer.parseInt(line[2]);
                a_d++;
            }
            Double a_f = (a_n+0.1)/(a_d+0.1+0.1);
            Double s_f = (s_n+0.1)/(s_d+0.1+0.1);
            Double r= a_f*s_f;
            StringBuilder sb = new StringBuilder();
            sb.append(a_f).append("\t");
            sb.append(s_f).append("\t");
            sb.append(r);
            context.write(key, new Text(sb.toString()));
        }
    }

    private Job getJobConf(final String input, final String output) throws IOException {

        final Job job = Job.getInstance(getConf());
        job.setJarByClass(sDBN.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(sDBNMapper.class);
        job.setReducerClass(sDBNReducer.class);
        return job;
    }

    @Override
    public int run(final String[] args) throws Exception {
        FileSystem fs = FileSystem.get(getConf());
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        final Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(final String[] args) throws Exception {
        final int ret = ToolRunner.run(new sDBN(), args);
        System.exit(ret);
    }
}