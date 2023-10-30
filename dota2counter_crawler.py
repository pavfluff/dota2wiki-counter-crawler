# Import libraries for crawling
import requests
from bs4 import BeautifulSoup

# Import PySpark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
import pyspark.sql.functions as F
from pyspark.sql import Window

## Creates spark session
sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName('Dota2 Crawler').getOrCreate()

# Creates dataframe with schema for the dataset
emptyRDD = spark.sparkContext.emptyRDD()

## Create schema for in loops
schema = StructType([
  StructField('type', StringType(), True),
  StructField('counter', StringType(), True),
  StructField('srcline', IntegerType(), True)
  ])

## Create schema for main dataframe
main_schema = StructType([
  StructField('hero', StringType(), True),
  StructField('segment', StringType(), True),
  StructField('type', StringType(), True),
  StructField('counter', StringType(), True),
  StructField('srcline', IntegerType(), True)
  ])

df = spark.createDataFrame(emptyRDD,schema=main_schema)

# get all heroes
page_hero = requests.get('https://dota2.fandom.com/wiki/Category:Heroes')
soup_hero = BeautifulSoup(page_hero.text, 'html.parser')
heroes = []
for i in soup_hero.find(class_="mw-content-ltr").find_all("a"):
    if '/Category:' not in i['href']:
        heroes.append(i.get_text())
    else:
        continue

for hero in heroes:
    print('Scraping hero page: '+hero)
    # initiate beautiful soup scraping on a page
    page = requests.get(f'https://dota2.fandom.com/wiki/{hero}/Counters')
    soup = BeautifulSoup(page.text, 'html.parser')

    # Pull all text from body div
    hero_name_list = soup.find_all(style='font-size:12pt;display:inline;')
    header_list = soup.find_all(class_='mw-headline')
    item_list = soup.find_all(class_='image-link')

    # function that outputs a range to categorize the type
    def rangeSrc(title,segment=None):
        if segment == None:
            return df_headers.filter(F.lower(F.col('counter'))==title).select('srcline').collect()[0]['srcline']
        else:
            return df_headers.filter((F.lower(F.col('counter'))==title) & (F.col('segment')==segment)).select('srcline').collect()[0]['srcline']

    # user-defined function for creating nested dictionary
    def dotaDict(type,scrape_list):
        text = []
        srcline = []
        types = []
        for h in scrape_list:
            types.append(type)
            text.append(h.get_text())
            srcline.append(h.sourceline)
        arr = zip(types,text,srcline)
        return arr

    # created nested dictionary
    ## header
    headers = dotaDict('header',header_list)
    ## heroes
    heroes = dotaDict('hero',hero_name_list)
    ## items
    items = dotaDict('items/skills',item_list)

    df_heroes = spark.createDataFrame(data=heroes,schema=schema)
    df_items = spark.createDataFrame(data=items,schema=schema)
    df_headers = spark.createDataFrame(data=headers,schema=schema)

    ## Create another column for segment
    # create threshold for segment
    bad_against = rangeSrc('bad against...')
    good_against = rangeSrc('good against...')
    works_well = rangeSrc('works well with...')

    # function for determining if the hero or item is a counter or not
    def segment(srcline):
        if srcline >= bad_against and srcline < good_against:
            return 'Bad against...'
        elif srcline >= good_against and srcline < works_well:
            return 'Good against...'
        elif srcline >= works_well:
            return 'Works well with...'
        else:
            pass
    segmentUDF = F.udf(lambda z: segment(z), StringType())

    # add segment
    df_heroes = df_heroes.withColumn('segment', segmentUDF('srcline'))
    df_items = df_items.withColumn('segment', segmentUDF('srcline'))
    df_headers = df_headers.withColumn('segment', segmentUDF('srcline'))

    # join 2 dfs
    df_conso = df_heroes.unionByName(df_items)
    df_conso = df_conso.withColumn('hero',F.lit(hero)).select('hero','segment','type','counter','srcline')
    df = df.unionByName(df_conso)
    print('Scraping done: '+hero)

# Adding primary key id
cols = ['id'] + df.columns
df = df.withColumn("monotonically_increasing_id", F.monotonically_increasing_id())
window = Window.orderBy(F.col('monotonically_increasing_id'))
df = df.withColumn('id', F.row_number().over(window)).select(cols)

# Renaming the cols according to the SQL server database names
df = df.withColumnRenamed('id','heroID') \
       .withColumnRenamed('hero','heroName') \
       .withColumnRenamed('segment','heroSegment') \
       .withColumnRenamed('type','heroType') \
       .withColumnRenamed('counter','heroCounter') \
       .withColumnRenamed('srcline','htmlCodeLine')

# Saving the data in local. Will use git push to update data in github
df = df.withColumn('dateLoad',F.date_format(F.current_timestamp(), 'yyyy-MM-dd'))
df.repartition(1).write.option("compression", "snappy")\
                       .partitionBy('dateload')\
                       .parquet('/home/maedora/Repositories/dota2wiki-counter-crawler/data/')