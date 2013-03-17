/*
 *
 * CS61C Spring 2013 Project 2: Small World
 *
 * Partner 1 Name:
 * Partner 1 Login:
 *
 * Partner 2 Name: Cedric Lamy
 * Partner 2 Login: cs61c-iy
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

        public String id; //id for the node
        public long parent = -1;
        public long[] edges; //example array of longs
        public boolean isStart = false;
        public long distance = 0;
        public long color = 0;

        public Node(String nodeId) {
            this.id = new String(nodeId);
        }

        public Node() {
            // does nothing
        }

        public String getId() {
            return this.id;
        }

        public long getParent() {
            return this.parent;
        }

        public void setParent(int par) {
            this.parent = par;
        }

        public long getDistance() {
            return this.distance;
        }

        public void setDistance(long dist) {
            this.distance = dist;
        }

        public long getColor() {
            return this.color;
        }

        public void setColor(long col) {
            this.color = col;
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
        public Node get() {
            return this;
        }


        // Serializes object - needed for Writable
        public void write(DataOutput out) throws IOException {

        int strlength = id.length();
	    out.writeInt(strlength);
	    for(int i = 0; i < strlength; i++) {
		out.writeChar(id.charAt(i));
	    }
            //out.writeChars(id+ "\n");

            // Example of serializing an array:
            
            // It's a good idea to store the length explicitly
            int length = 0;

            if (edges != null){
                length = edges.length;
            }

           
            // always write the length, since we need to know
            // even when it's zero
            out.writeInt(length);
            out.writeBoolean(this.isStart());
            out.writeLong(this.getDistance());
            out.writeLong(this.getColor());
            // now write each long in the array
            for (int i = 0; i < length; i++){
                out.writeLong(edges[i]);
            }
        }

        // Deserializes object - needed for Writable
        public void readFields(DataInput in) throws IOException {
            // example reading an int from the serialized object
	    int strlength = in.readInt();
	    this.id = "";
	    for(int i = 0; i < strlength; i++) {
		this.id += in.readChar();
	    }
            //this.id = in.readLine();

            // example reading length from the serialized object
            int length = in.readInt();
            this.isStart = in.readBoolean();
            this.setDistance(in.readLong());
            this.setColor(in.readLong());
            // Example of rebuilding the array from the serialized object
            this.edges = new long[length];
            
            for(int i = 0; i < length; i++){
                edges[i] = in.readLong();
            }
        }

        public String toString() {
            // We highly recommend implementing this for easy testing and
            // debugging. This version just returns an empty string.
            StringBuffer sb = new StringBuffer();
            sb.append("the node id is: \t" + this.id + "\nthe node isStart is:\t" + this.isStart + "\nthe edges is:\t");
            if (edges != null) {
                for (long edge : this.edges) {
                    sb.append(edge + ", ");
                }
            }
            sb.append("\ncolor:\t" + this.color);
            sb.append("\ndistance:\t" + this.distance);
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
        Text, Node> {

        public long denom;

        public boolean isStart(long denom) {
            if (Math.random() < (1.0 / denom)) {
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
            String alpha = key + "--" + key;
            ArrayList<Long> edges = new ArrayList<Long>();
            Node node = new Node(alpha);
            
            int i = 0;
            for (LongWritable value : values) {
                edges.add(value.get());
            }

            long[] arrayEdges = new long[edges.size()];

            for (Long edge : edges) {
                arrayEdges[i] = (long) edge;
                i++;
            }

            //choose start 
            if (isStart(denom)) {
                node.setEdges(arrayEdges);
                node.setColor(1);
                node.setDistance(0);
                node.setStart(true);
            }
	    theUniverse.put(node.getId(), node);
            context.write(new Text(alpha), node);
        }
    }

    /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class SearchMap extends Mapper<Text, Node, 
        Text, Node> {

        @Override
        public void map(Text key, Node value, Context context)
                throws IOException, InterruptedException {
            Node node = value.get();
            String alpha = key.toString();
            if (node.getColor() == 1) {
                if (node.getEdges() != null) {
                    for (long v : node.getEdges()) {
                        String beta = alpha.split("--")[0];
                        beta += "--" + v;
                        Node vnode = new Node(beta);
                        vnode.setDistance(node.getDistance() + 1);
                        vnode.setColor(1);
                        context.write(new Text(vnode.getId()), vnode);
                    }
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
    public static class SearchReduce extends Reducer<Text, Node, 
        Text, Node> {

        public void reduce(Text key, Iterable<Node> values, 
            Context context) throws IOException, InterruptedException {
            long edges[] = null;
            long distance = Long.MAX_VALUE;
            long color = 0;

            for (Node u : values) {
                //find the minimum distance
                if (u.getDistance() < distance) {
                    distance = u.getDistance();
		}
                //find the darkest color
		if (u.getColor() > color) {
                    color = u.getColor();
                }
            }
	    String brandon = key.toString().split("--")[1];
	    edges = theUniverse.get(brandon + "--" + brandon).getEdges();

            Node node = new Node(key.toString());
            node.setEdges(edges);
	    node.setColor(color);
	    node.setDistance(distance);
            context.write(key, node);
        }
    }

     /* The first mapper. Part of the graph loading process, currently just an 
     * identity function. Modify as you wish. */
    public static class HistoMap extends Mapper<Text, Node, 
        LongWritable, LongWritable> {

        @Override
        public void map(Text key, Node value, Context context)
                throws IOException, InterruptedException {
            Node node = value.get();
            context.write(new LongWritable(node.getDistance()), new LongWritable(1L));
        }
    }

      /* The first reducer. This is also currently an identity function (although it
     * does break the input Iterable back into individual values). Modify it
     * as you wish. In this reducer, you'll also find an example of loading
     * and using the denom field.  
     */
    public static class HistoReduce extends Reducer<LongWritable, LongWritable, 
        LongWritable, LongWritable> {

        public void reduce(LongWritable key, Iterable<LongWritable> values, 
            Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new LongWritable(key.get()), new LongWritable(sum));
        }
    }

    public static void main(String[] rawArgs) throws Exception {
        GenericOptionsParser parser = new GenericOptionsParser(rawArgs);
        Configuration conf = parser.getConfiguration();
        String[] args = parser.getRemainingArgs();

	theUniverse = new HashMap<String, Node>();


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
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

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
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Node.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Node.class);

            // You'll want to modify the following based on what you call
            // your mapper and reducer classes for the BFS phase.
            job.setMapperClass(SearchMap.class); // currently the default Mapper
            job.setReducerClass(SearchReduce.class); // currently the default Reducer

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
        job.setMapperClass(HistoMap.class); // currently the default Mapper
        job.setReducerClass(HistoReduce.class); // currently the default Reducer

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // By declaring i above outside of loop conditions, can use it
        // here to get last bfs output to be input to histogram
        FileInputFormat.addInputPath(job, new Path("bfs-"+ i +"-out"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
    static HashMap<String, Node> theUniverse;
}
