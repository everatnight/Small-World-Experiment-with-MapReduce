/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name:
 * Partner 1 Login:
 *
 * Partner 2 Name:
 * Partner 2 Login:
 *
 * REMINDERS: 
 *
 * 1) YOU MUST COMPLETE THIS PROJECT WITH A PARTNER.
 * 
 * 2) DO NOT SHARE CODE WITH ANYONE EXCEPT YOUR PARTNER.
 * EVEN FOR DEBUGGING. THIS MEANS YOU.
 *
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.Math;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SmallWorld {
    // Maximum depth for any breadth-first search
    public static final int MAX_ITERATIONS = 20;

    // Example writable type
    public static class Node implements Writable {

        public long id; //id for the node
        public int color = 0;  //node color
        public long parent = Integer.MAX_VALUE;
        public long distance = Integer.MAX_VALUE;
        public long[] edges; //example array of longs
        public boolean isStart = false;

        public Node(long nodeId) {
            this.id = nodeId;
        }

        public Node() {
            // does nothing
        }

        public long getId() {
            return this.id;
        }

        public long getParent() {
            return this.parent;
        }

        public void setParent(int parent) {
            this.parent = parent;
        }

        public long getDistance() {
            return this.distance;
        }

        public int getColor() {
            return this.color;
        }

        public void setColor(int color) {
            this.color = color;
        }

        public long[] getEdges() {
            return this.edges;
        }

        public void setEdges(long[] vertices) {
            this.edges = vertices;
        }

        public void setStart(boolean start) {
            this.isStart = start;
        }

        public boolean isStart() {
            return this.isStart;
        }

        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {
            out.writeLong(id);

            // Example of serializing an array:
            
            // It's a good idea to store the length explicitly
            long length = 0;

            if (edges != null){
                length = edges.length;
            }

            // always write the length, since we need to know
            // even when it's zero
            out.writeLong(length);

            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(edges[i]);
            }

            out.writeLong(distance); //save distance
            out.writeInt(color); //save color
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // example reading an int from the serialized object
            this.id = in.readLong();

            // example reading length from the serialized object
            long length = in.readLong();

            // Example of rebuilding the array from the serialized object
            this.edges = new long[length];
            
            for(int i = 0; i < length; i++){
                edges[i] = in.readLong();
            }

            this.distance = in.readLong();
            this.color = in.readInt()
        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            StringBuffer sb = new StringBuffer();
            sb.append("Node id is " + this.id + " color is " + this.color
                + "distance is " + this.distance + "parent is " + this.parent + "edges is ");
            for (long edge : edges) {
                sb.append(edge + ", ");
            }
            sb.append("\n");
            return sb.toString();
        }

    }


    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class LoaderMap extends Mapper<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        @Override
        public void map(LongWritable key, LongWritable value, Context context)
                throws IOException, InterruptedException {

            // example of getting value passed from main
            int inputValue = Integer.parseInt(context.getConfiguration().get("inputValue"));
            context.write(key, value);
        }
    }


    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class LoaderReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, Node> {

        public long denom;

        public boolean isStart(denom) {
            if (Math.random() < (1 / denom)) {
                return true;
            }
            return false;
        }

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            // We can grab the denom field from context: 
            denom = Long.parseLong(context.getConfiguration().get("denom"));

            // You can print it out by uncommenting the following line:
            // System.out.println(denom);

            //the edges ArrayList.
            ArrayList<Long> edges = new ArrayList<Long>();
            Node node = new Node(key.get());

            for (LongWritable value : values) {
                edges.add(value.get());
            }
            node.setEdges((edges.toArray(new long[edges.size()])); //todo test if need cast
            
            //choose start 
            if (isStart(denom)) {
                node.setColor(1);
                node.setDistance(0);
                node.setStart(true);
            }
            context.write(key, node);
        }
    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class SearchMap extends Mapper<LongWritable, Node, 
        LongWritable, Node> {

        @Override
        public void map(LongWritable key, Node value, Context context)
                throws IOException, InterruptedException {
            Node node = value.get();

            if (node.getColor() == 1) {
                for (long v : node.getEdges()) {
                    Node vnode = new Node(v);
                    vnode.setDistance(node.getDistance() + 1);
                    vnode.setColor(1);
                    context.write(new LongWritable(vnode.getId()), vnode);
                }
                node.setColor(2);
            }
            context.write(key, value);
        }
    }

    /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class SearchReduce extends Reducer<LongWritable, Node, 
        LongWritable, Node> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            long edges[];
            long distance = Long.MAX_VALUE;
            int color = 0;

            for (Node u : values) {
                if (u.getEdges().size() > 0) {
                    edges = u.getEdges();
                }
                if (u.getDistance() < distance) {
                    distance = u.getDistance();
                }
                if (u.getColor() > color) {
                    color = u.getColor();
                }
            }
            Node n = new Node(key.get());
            n.setDistance(distance);
            n.setEdges(edges);
            n.setColor(color);
            context.write(key, node);
        }
    }














    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

        // Pass in denom command line arg:
        conf.set("denom", args[2]);

        // Sample of passing value from main into Mappers/Reducers using
        // conf. You might want to use something like this in the BFS phase:
        // See LoaderMap for an example of how to access this value
        conf.set("inputValue", (new Integer(5)).toString());

        // Setting up mapreduce job to load in graph
        Job job = new Job(conf, "load graph");

        job.setJarByClass(SmallWorld.class);

        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(LoaderMap.class);
        job.setReducerClass(LoaderReduce.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Input from command-line argument, output to predictable place
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path("bfs-0-out"));

        // Actually starts job, and waits for it to finish
        job.waitForCompletion(true);

        // Repeats your BFS mapreduce
        int i = 0;
        while (i < MAX_ITERATIONS) {
            job = new Job(conf, "bfs" + i);
            job.setJarByClass(SmallWorld.class);

            // Feel free to modify these four lines as necessary:
            job.setMapOutputKeyClass(LongWritable.class);
            job.setMapOutputValueClass(LongWritable.class);
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(Mapper.class); // currently the default Mapper
            job.setReducerClass(Reducer.class); // currently the default Reducer

            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);

            // Notice how each mapreduce job gets gets its own output dir
            FileInputFormat.addInputPath(job, new Path("bfs-" + i + "-out"));
            FileOutputFormat.setOutputPath(job, new Path("bfs-"+ (i+1) +"-out"));

            job.waitForCompletion(true);
            i++;
        }

        // Mapreduce config for histogram computation
        job = new Job(conf, "hist");
        job.setJarByClass(SmallWorld.class);

        // Feel free to modify these two lines as necessary:
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);

        // DO NOT MODIFY THE FOLLOWING TWO LINES OF CODE:
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // You'll want to modify the following based on what you call your
        // mapper and reducer classes for the Histogram Phase
        job.setMapperClass(Mapper.class); // currently the default Mapper
        job.setReducerClass(Reducer.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
