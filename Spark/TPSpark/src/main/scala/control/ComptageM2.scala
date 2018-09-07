package control
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

import org.apache.log4j.Logger
import org.apache.log4j.Level

//2.	Créer une classe pour représenter un Client. On devra très probablement utiliser une case class.
case class Client(clientid: Integer, nom: String, ville: String, province: String, postalcode: String)

object ComptageM2 {
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    
    val conf = new SparkConf()
    conf.setAppName("SparkSQL").setMaster("local")
      
    //Creation du Context Spark  
    val sc = new SparkContext(conf)
      
      
    // 1.	Créer un SQLContext à partir du context Spark existant
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    // conversion RDD vers DataFrame.
    import sqlContext.implicits._

    //au chargement du data au niveau du DataFrame
    val clientText = sc.textFile("file:///home/cloudera/client.csv")
    clientText.first()
    
    // creation RDD d'objets Client
    val client = clientText.map(_.split(",")).map(p => Client(p(0).toInt,p(1),p(2),p(3),p(4)))
    
    // 3.	Créer un dataframe qui va contenir les objets clients lus à partir d’un dataset représentant le fichier des clients. 
    //On appelera le dataframe dfClients.
    val dfClients = client.toDF()
    
    // 4.	Faire en sorte d’enregistrer le dataframe comme une table –clients-
    dfClients.registerTempTable("client")
    var results = sqlContext.sql("SELECT * FROM client")
    results.show() 
    
    //5.	Afficher le contenu du dataframe dfClients.
    dfClients.show()
    
    // 6.	Afficher le schéma du dataframe dfClients.
    dfClients.printSchema()
    
    // 7.	Procéder à un SELECT de la colonne –nom- seulement
    dfClients.select("nom").show()
    
    //8.	Procéder à un SELECT des colonnes –nom- et -ville-
    dfClients.select("nom","ville")

    // 9.	Afficher le détail du client ayant un ID 30
    val cid= dfClients.filter("clientid= 30")
    cid.show()
    
    //10.	Procéder au groupage des clients par code postal 
    results = sqlContext.sql("SELECT postalcode, count(clientid) FROM client GROUP BY postalcode")
    results.show() 

  
  }
  
}