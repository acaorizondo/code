/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cu.uh.ddi.ejemplo_spark;

import cu.uh.ddi.spark.util.SparkSessionUtil;
import cu.uh.ddi.spark.util.DataSourceFileUtil;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.tuning.ParamGridBuilder;
import org.apache.spark.ml.tuning.TrainValidationSplit;
import org.apache.spark.ml.tuning.TrainValidationSplitModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

/**
 *
 * @author Arturo
 * Ejemplo de uso de una red neuronal en un problema de clasificación binaria usando Spark-mllib
 * se emplea el fichero de datos "Horse Colic"
 */
public class MultilayerPerceptronClassifierExample {
    private static SparkSession spark;
    private static Dataset<Row> primary_data;
    
    public static void main (String[] args){
      
      // nombre de la aplicación
      String appName = "Regresión lineal";
      // estructura del encabezado del fichero de datos (necesaria cuando el fichero original no está en formato csv)
      String file_header = "surgery?,Age,Hospital Number,rectal temperature,pulse,respiratory rate,temperature of extremities,peripheral pulse,mucous membranes,capillary refill time,pain,peristalsis,abdominal distension,nasogastric tube,nasogastric reflux,nasogastric reflux PH,rectal examination - feces,abdomen,packed cell volume,total protein,abdominocentesis appearance,abdomcentesis total protein,outcome,surgical lesion?,lesion_1,lesion_2,lesion_3,cp_data";
      //ubicación del fichero de datos
      String original_file_location ="D:\\Curso\\Apache Spark\\Tema 4\\Datasets Univ. California Irvine\\Datasets Univ. California Irvine\\Horse Colic Data Set\\horse-colic.data";
     
      //crear el SparkSession
      SparkSessionUtil ss = new SparkSessionUtil(appName);
      spark = ss.getSparkSession();
      
      //cargar fichero de datos en formato csv
      String type_of_file = "data_file"; // indicar el tipo de fichero de datos (data_file, csv_file)
      DataSourceFileUtil dsFile = new DataSourceFileUtil(spark, file_header, original_file_location, type_of_file);
      primary_data = dsFile.getDataset();
      
      operations();
      spark.stop();
    }
    
    public static void operations(){
      
        Dataset<Row> ds = DataPreparation.dataPreparation(primary_data);
        ds.show();
               
        Dataset<Row>[] splits = ds.randomSplit(new double[] {0.7, 0.3}, 1234L);
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];
      
        redNeuronal(trainingData, testData);

    }
    
    public static void redNeuronal(Dataset<Row> trainingData, Dataset<Row> testData)
    {
       //Preparamos las siguientes transformaciones, para datos nominales
        StringIndexer surgeryIndexer = new StringIndexer().setInputCol("surgery?").setOutputCol("surgery?Index").setHandleInvalid("keep");
        StringIndexer ageIndexer = new StringIndexer().setInputCol("Age").setOutputCol("AgeIndex").setHandleInvalid("keep");
        StringIndexer abdomenIndexer = new StringIndexer().setInputCol("abdomen").setOutputCol("abdomenIndex").setHandleInvalid("keep");
        StringIndexer outcomeIndexer = new StringIndexer().setInputCol("label").setOutputCol("outcomeIndex").setHandleInvalid("keep");

       //Dataset<Row> ds_2 = ds; 
       //arquitectura de la red neuronal
       int[] layers = new int[]{3,11,5,2};
       //configuración de la red neuronal
       MultilayerPerceptronClassifier redNeuronal = new MultilayerPerceptronClassifier()
                .setLayers(layers)
                .setBlockSize(128)
                .setSeed(1234L)
                .setMaxIter(100);
       redNeuronal.setFeaturesCol("featuresNormalized");
       redNeuronal.setLabelCol("outcomeIndex");
        
       VectorAssembler assembler = new VectorAssembler().setInputCols(new String[]{"surgery?Index","AgeIndex","abdomenIndex"})
                                                        .setOutputCol("features");
        
        Normalizer normalizer = new Normalizer()
                                    .setInputCol("features")
                                    .setOutputCol("featuresNormalized")
                                    .setP(1.0);
       
        Pipeline pipelineMLP = new Pipeline().setStages(new PipelineStage[]{surgeryIndexer,ageIndexer,outcomeIndexer,abdomenIndexer,
                                                                            assembler,
                                                                            normalizer,
                                                                            redNeuronal}
                                                        );
        
        //buscar hiper-parámetros
        ParamGridBuilder paramGridMLP = new ParamGridBuilder();
        paramGridMLP.addGrid(redNeuronal.stepSize(), new double[]{0.01, 0.001,0.0015});
       
        //Buscamos hiper-parámetros y ejecutamos el pipeline
        TrainValidationSplit trainValidationSplitMLP = new TrainValidationSplit()
                .setEstimator(pipelineMLP)
                .setEstimatorParamMaps(paramGridMLP.build())
                //Para el evaluador podemos elegir: BinaryClassificationEvaluator, ClusteringEvaluator, MulticlassClassificationEvaluator, RegressionEvaluator
                .setEvaluator(new BinaryClassificationEvaluator());
        
        TrainValidationSplitModel modelMLP = trainValidationSplitMLP.fit(trainingData);
        Dataset<Row> resultMLP = modelMLP.transform(testData);
        
        resultMLP.show();
        //Analizar métricas de rendimiento Accuracy y Confusion matrix
        MulticlassMetrics metrics = new MulticlassMetrics(resultMLP.select("prediction", "outcomeIndex"));

        System.out.println("Test set accuracy = " + metrics.accuracy());
        System.out.println("Confusion matrix = \n" + metrics.confusionMatrix());
        
    } 
    
}
