package spark.batch;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import javax.print.Doc;

import static jersey.repackaged.com.google.common.base.Preconditions.checkArgument;

public class CountTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CountTask.class);

    public static void main(String[] args) {
        checkArgument(args.length > 1, "Please provide the path of input file and output dir as parameters.");
        new CountTask().run(args[0], args[1]);
    }

    private static final String DB_URI = "mongodb://alaa:alaajebcar@ac-eo6n1um-shard-00-00.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-01.nxtedqb.mongodb.net:27017,ac-eo6n1um-shard-00-02.nxtedqb.mongodb.net:27017/?ssl=true&replicaSet=atlas-tli1u5-shard-0&authSource=admin&retryWrites=true&w=majority";
    private static final String DB_NAME = "Accidents";
    private static final String COLLECTION_NAME = "locations";

    public void run(String inputFilePath, String outputDir) {
//        MongoClientURI uri = new MongoClientURI(DB_URI);
//        MongoClient mongoClient = new MongoClient(uri);
//
//        MongoDatabase database = mongoClient.getDatabase(DB_NAME);
//
//        MongoCollection<Document> ageCollection = database.getCollection("age");
//        MongoCollection<Document> locCollection = database.getCollection("location-group");
//        MongoCollection<Document> timeCollection = database.getCollection("time");

        String master = "local[*]";
        SparkConf conf = new SparkConf()
                .setAppName(CountTask.class.getName())
                .setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> textFile = sc.textFile(inputFilePath);

        JavaPairRDD<String, Integer> locationCounts = textFile
                .mapToPair(line -> {
                    String[] fields = line.split(";");
                    String location = fields[0];
                    return new Tuple2<>(location, 1);
                })
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> timeCounts = textFile
                .mapToPair(line -> {
                    String[] fields = line.split(";");
                    String time = fields[1];
                    return new Tuple2<>(time, 1);
                })
                .reduceByKey((a, b) -> a + b);

        JavaPairRDD<String, Integer> ageCounts = textFile
                .mapToPair(line -> {
                    String[] fields = line.split(";");
                    String age = fields[2];
                    System.out.println(new Tuple2<>(age, 1));

                    return new Tuple2<>(age, 1);
                })
                .reduceByKey((a, b) -> a + b);
        ageCounts.foreach(x -> {

            MongoClientURI uri = new MongoClientURI(DB_URI);
            MongoClient mongoClient = new MongoClient(uri);

            MongoDatabase database = mongoClient.getDatabase(DB_NAME);

            MongoCollection<Document> ageCollection = database.getCollection("age");
            Document ageDocument = new Document();
            ageDocument.put("age", x._1);
            ageDocument.put("count", x._2);

            ageCollection.insertOne(ageDocument);
            mongoClient.close();
        });
//
        locationCounts.foreach(x -> {
            MongoClientURI uri = new MongoClientURI(DB_URI);
            MongoClient mongoClient = new MongoClient(uri);

            MongoDatabase database = mongoClient.getDatabase(DB_NAME);

            MongoCollection<Document> locCollection = database.getCollection("location-group");
            Document locationDocument = new Document();
            locationDocument.put("location", x._1);
            locationDocument.put("count", x._2);
            locCollection.insertOne(locationDocument);
            mongoClient.close();

        });
//
        timeCounts.foreach(x -> {
            MongoClientURI uri = new MongoClientURI(DB_URI);
            MongoClient mongoClient = new MongoClient(uri);

            MongoDatabase database = mongoClient.getDatabase(DB_NAME);

            MongoCollection<Document> timeCollection = database.getCollection("time");

            Document timeDocument = new Document();
            timeDocument.put("age", x._1);
            timeDocument.put("count", x._2);
            timeCollection.insertOne(timeDocument);
            mongoClient.close();

        });

//        mongoClient.close();

        locationCounts.saveAsTextFile(outputDir + "/locationCounts");
        timeCounts.saveAsTextFile(outputDir + "/timeCounts");
        ageCounts.saveAsTextFile(outputDir + "/ageCounts");
    }
}