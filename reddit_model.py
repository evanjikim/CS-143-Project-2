from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions as F

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator

from pyspark.sql.functions import udf, col, unix_timestamp
from pyspark.sql.types import StringType, ArrayType, IntegerType, DateType
from pyspark.ml.feature import CountVectorizer, CountVectorizerModel
import cleantext

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder, CrossValidatorModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator


states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']

# TASK 4
def get_ngrams(text):
    ngrams = []
    temp = cleantext.sanitize(text)
    ngrams = temp[1] + ' ' + temp[2] + ' ' + temp[3]
    return ngrams.split(' ')


def main(context):
    # TASK 1
    try:
        commentsDF = context.read.load('comments.parquet')
        submissionsDF = context.read.load('submissions.parquet')
        labeled_dataDF = context.read.load('label.parquet')
    except:
        commentsDF = sqlContext.read.json('comments-minimal.json.bz2')
        submissionsDF = sqlContext.read.json('submissions.json.bz2')
        labeled_dataDF = sqlContext.read.load('labeled_data.csv', format = 'csv', sep = ',',header="true")
        commentsDF.write.parquet('comments.parquet')
        submissionsDF.write.parquet('submissions.parquet')
        labeled_dataDF.write.parquet('label.parquet')


    # TASK 2
    joined_data = commentsDF.join(labeled_dataDF, commentsDF.id == labeled_dataDF.Input_id, 'inner').select(col('id'), col('body'), col('labeldjt'))
    
    # TASK 4,5
    ngrams_udf = udf(get_ngrams, ArrayType(StringType()))
    joined_col = joined_data.withColumn('ngrams', ngrams_udf(joined_data['body']))


    try:
        model = CountVectorizerModel.load('cv.model')

    except:
        # task 6A
        cv = CountVectorizer(inputCol = 'ngrams', outputCol = "features", binary = True)
        model = cv.fit(joined_col)
        vectors = model.transform(joined_col)

        # task 6B
        positive_udf = udf(lambda x: 1 if x == '1' else 0, IntegerType())
        negative_udf = udf(lambda x: 1 if x == '-1' else 0, IntegerType())
        vectors = vectors.withColumn('positive', positive_udf(col('labeldjt')))
        vectors = vectors.withColumn('negative', negative_udf(col('labeldjt')))

        pos = vectors.select(col('positive').alias('label'), col('features'))
        neg = vectors.select(col('negative').alias('label'), col('features'))
        pos.write.parquet('positive_ROC.parquet')
        neg.write.parquet('negative_ROC.parquet')
        model.save('cv.model')
    try:
        posModel = CrossValidatorModel.load('pos.model')
        negModel = CrossValidatorModel.load('neg.model')
    except:
        # Task 7
        # Initialize two logistic regression models.
        # Replace labelCol with the column containing the label, and featuresCol with the column containing the features.
        poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
        neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10)
        # This is a binary classifier so we need an evaluator that knows how to deal with binary classifiers.
        posEvaluator = BinaryClassificationEvaluator()
        negEvaluator = BinaryClassificationEvaluator()
        # There are a few parameters associated with logistic regression. We do not know what they are a priori.
        # We do a grid search to find the best parameters. We can replace [1.0] with a list of values to try.
        # We will assume the parameter is 1.0. Grid search takes forever.
        posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
        negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()
        # We initialize a 5 fold cross-validation pipeline.
        posCrossval = CrossValidator(
            estimator=poslr,
            evaluator=posEvaluator,
            estimatorParamMaps=posParamGrid,
            numFolds=5)
        negCrossval = CrossValidator(
            estimator=neglr,
            evaluator=negEvaluator,
            estimatorParamMaps=negParamGrid,
            numFolds=5)
        # Although crossvalidation creates its own train/test sets for
        # tuning, we still need a labeled test set, because it is not
        # accessible from the crossvalidator (argh!)
        # Split the data 50/50
        posTrain, posTest = pos.randomSplit([0.5, 0.5])
        negTrain, negTest = neg.randomSplit([0.5, 0.5])

        # Train the models
        print("Training positive classifier...")
        posModel = posCrossval.fit(posTrain)
        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        posModel.save("pos.model")
        print("Training negative classifier...")
        negModel = negCrossval.fit(negTrain)
        # Once we train the models, we don't want to do it again. We can save the models and load them again later.
        negModel.save("neg.model")

    # Task 8,9
    try:
        finalDF = context.read.load('final.parquet')
    except:
        extract_id_udf = udf(lambda x:x[3:], StringType())
        comments = commentsDF.select(col('id').alias('comment_id'), extract_id_udf(col('link_id')).alias('link_id'), col('created_utc'), col('body'), col('author_flair_text'), col('score').alias('comment_score'))
        submissions = submissionsDF.select(col('id').alias('submission_id'), col('title'), col('score').alias('submission_score'))
        finalDF = comments.join(submissions, comments.link_id == submissions.submission_id, 'inner')
        #sampling 20%
        finalDF = finalDF.sample(False, 0.02, None)
        pos_threshold_udf = udf(lambda x: 1 if x[1] > 0.2 else 0, IntegerType())
        neg_threshold_udf = udf(lambda x: 1 if x[1] > 0.25 else 0, IntegerType())
        finalDF = finalDF.filter("body NOT LIKE '%/s%' and body NOT LIKE '&gt;%'")
        finalDF = finalDF.withColumn('ngrams', ngrams_udf(finalDF['body']))
        finalDF = model.transform(finalDF)
        posResult = posModel.transform(finalDF)
        temp = posResult.withColumn('pos', pos_threshold_udf(posResult['probability']))
        temp = temp.select(col('comment_id'),col('link_id'),col('created_utc'), col('body'), col('author_flair_text'), col('comment_score'),col('submission_id'), col('title'), col('submission_score'),col('ngrams'),col('pos'))
        temp = model.transform(temp)
        negResult = negModel.transform(temp)
        temp = negResult.withColumn('neg', neg_threshold_udf(negResult['probability']))
        finalDF = temp.select(col('comment_id'),col('link_id'),col('created_utc'), col('body'), col('author_flair_text'), col('comment_score'),col('submission_id'), col('title'), col('submission_score'),col('ngrams'),col('pos'),col('neg'))
        finalDF.write.parquet('final.parquet')
    # Task 10
    # percentage of positive and negative comments
    try:
        task1 = context.read.load('percentage_value.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        total_rows = finalDF.count()
        total_pos_comments = finalDF.filter(col('pos') == '1').count()
        total_neg_comments = finalDF.filter(col('neg') == '1').count()

        pos_percentage = total_pos_comments / total_rows
        neg_percentage = total_neg_comments / total_rows

        values = [{'Total Rows': total_rows, 'Percentage of Positive Comments': pos_percentage, 'Percentage of Negative Comments': neg_percentage}]
        task1 = sqlContext.createDataFrame(values)
        task1.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("percentage_value.csv")
    #percent over date
    try:
        task2 = context.read.load('time_data.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        task2 = finalDF.withColumn('date', F.from_unixtime(col('created_utc')).cast(DateType()))
        task2 = task2.groupBy('date').agg((F.sum('pos') / F.count('pos')).alias('Positive'), (F.sum('neg') / F.count('neg')).alias('Negative'))
        task2.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("time_data.csv")
    #percent over states
    try:
        task3 = context.read.load('state_data.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        state = sqlContext.createDataFrame(states, StringType())
        task3 = finalDF.groupBy('author_flair_text').agg((F.sum('pos') / F.count('pos')).alias('Positive'), (F.sum('neg') / F.count('neg')).alias('Negative'))
        task3 = task3.join(state, task3.author_flair_text == state.value, 'inner').na.drop(subset=['value']).select(col('author_flair_text').alias('state'),col('Positive'),col('Negative'))
        task3.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state_data.csv")
    #percent over submission score
    try:
        task4 = context.read.load('submission_score.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        task4 = finalDF.groupBy('submission_score').agg((F.sum('pos') / F.count('pos')).alias('Positive'), (F.sum('neg') / F.count('neg')).alias('Negative'))
        task4.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("submission_score.csv")
    #percent over commet score
    try:
        task5 = context.read.load('comment_score.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        task5 = finalDF.groupBy('comment_score').agg((F.sum('pos') / F.count('pos')).alias('Positive'), (F.sum('neg') / F.count('neg')).alias('Negative'))
        task5.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")
    #list top 10 stories of each sentiment
    try:
        top_positive = context.read.load('top_positive.csv/*.csv',format = 'csv', sep = ',',header="true")
        top_negative = context.read.load('top_negative.csv/*.csv',format = 'csv', sep = ',',header="true")
    except:
        top_positive = finalDF.groupBy('title').agg((F.sum('pos') / F.count('pos')).alias('Percentage')).orderBy(F.desc('Percentage')).limit(10)
        top_negative = finalDF.groupBy('title').agg((F.sum('neg') / F.count('neg')).alias('Percentage')).orderBy(F.desc('Percentage')).limit(10)
        top_positive.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("top_positive.csv")
        top_negative.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("top_negative.csv")


if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)
