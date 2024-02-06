
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors



spark = SparkSession.builder.appName("RidgeRegressionA3").getOrCreate()


trainingData = spark.read.format("libsvm").load('train.txt')


ridge = LinearRegression(maxIter=100, regParam=0.5, elasticNetParam=0, fitIntercept=True)

model = ridge.fit(trainingData)


print("Weights:", model.coefficients)
print("Intercept:", model.intercept)

features_set = [32.9, 74, 257.50, 70.00, 40.8, 132.4, 108.5, 107.1, 59.3, 42.2, 24.6, 35.7, 30.0, 25.9]
testRow = [(0, Vectors.dense(features_set))]
testDF = spark.createDataFrame(testRow, ["label", "features"])

predictions = model.transform(testDF)
predictions.select("prediction").show()
spark.stop()
