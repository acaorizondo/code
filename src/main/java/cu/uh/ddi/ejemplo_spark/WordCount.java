/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uh.ddi.ejemplo_spark;

import java.util.Arrays;
import java.util.List;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 *
 * @author Arturo
 */
public class WordCount {
    
    public static void  main (String[] args){
        String file_location = "C:\\Users\\Arturo\\Documents\\revisión de artículo.txt";
        contarPalabras(file_location);
    }
    
    public static void contarPalabras(String ubicacion){
       SparkSession spark = SparkSession
                            .builder()
                            .appName("Contador de palabras")
                            .master("local[*]")
                            .getOrCreate();
       spark.sparkContext().setLogLevel("ERROR");
       JavaRDD<String> lines = spark.read().textFile(ubicacion).javaRDD();
       JavaRDD<String> words = lines.flatMap(s->Arrays.asList(s.split(" ")).iterator());
       JavaPairRDD<String, Integer> ones = words.mapToPair(s->new Tuple2<>(s,1));
       JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1,i2)->i1+i2);
       List<Tuple2<String,Integer>> output = counts.collect();
       for (Tuple2<?,?> tuple: output){
         System.out.println(tuple._1()+" "+tuple._2());
       }
       spark.stop();
    }
}
