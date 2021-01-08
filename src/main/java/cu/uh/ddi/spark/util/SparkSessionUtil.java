/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uh.ddi.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author Arturo
 */
public class SparkSessionUtil {
    private static final String MASTER_CONFIG = "local[*]";
    private final SparkSession ss;
    
    public SparkSessionUtil(String appName){
        ss = createSession(appName);
    }
    
    public SparkSession getSparkSession(){
    
        return ss;
    }
    
    public static SparkSession createSession(String appName){
        final SparkConf conf = new SparkConf().setAppName(appName).setMaster(MASTER_CONFIG);
        final JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");
        
        SparkSession ss = SparkSession
                             .builder()
                             .sparkContext(sc.sc())
                             .getOrCreate();
        return ss;
    }
     
    
}
