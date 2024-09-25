<h2 align="center">
  Welcome to My Data Engineering Project!
  <img src="https://media.giphy.com/media/hvRJCLFzcasrR4ia7z/giphy.gif" width="28">
</h2>


<!-- Intro  -->
<h3 align="center">
        <samp>&gt; Hey There!, I am
                <b><a target="_blank" href="https://yourwebsite.com">Shubham Dalvi</a></b>
        </samp>
</h3>

<p align="center"> 
  <samp>
    <br>
    「 I am a data engineer with a passion for big data , distributed computing and data visualization 」
    <br>
    <br>
  </samp>
</p>

<div align="center">
  <a href="https://git.io/typing-svg">
    <img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&random=false&width=435&lines=Analyst+%40+Accenture;3+years+of+IT+experience;+Passionate+Data+Engineer;Spark+%7C+Azure+%7C+Power+BI+%7C+Snowflake+%7C+Airflow" alt="Typing SVG">
  </a>
</div>

<p align="center">
 <a href="https://linkedin.com/in/yourprofile" target="_blank">
  <img src="https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white" alt="yourprofile"/>
 </a>
</p>
<br />

<!-- About Section -->
 # About me
 
<p>
 <img align="right" width="350" src="/assets/programmer.gif" alt="Coding gif" />
  
 ✌️ &emsp; Enjoy solving data problems <br/><br/>
 ❤️ &emsp; Passionate about big data technologies, distributed systems and data visualizations<br/><br/>
 📧 &emsp; Reach me : dshubhamp1999@gmail.com<br/><br/>

</p>

<br/>
<br/>
<br/>

## Skills and Technologies

![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)
![Matplotlib](https://img.shields.io/badge/Matplotlib-013243?style=for-the-badge&logo=matplotlib&logoColor=white)
![Jupyter](https://img.shields.io/badge/Jupyter-F37626?style=for-the-badge&logo=jupyter&logoColor=white)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)
![VSCode](https://img.shields.io/badge/Visual_Studio-0078d7?style=for-the-badge&logo=visual%20studio&logoColor=white)

<br/>

# Traffic Analysis and Event Day Impact

This project analyzes traffic data to gain insights on vehicle flow during normal days and game event days. The project involves processing a raw traffic data file, cleaning the data, and combining it with event day information to extract useful insights. 

## Project Overview

The goal of this project is to:
- Read and process traffic data from a text file.
- Transform and clean the data using PySpark.
- Aggregate the data to calculate vehicle counts per day.
- Integrate event day data to analyze the difference in traffic during regular days and game days.
- Provide summary statistics and insights.

## Technologies Used

- PySpark
- Spark SQL
- Python

## Data Sources

1. **Traffic Data (`Dodgers.data`)**: Contains timestamp and number of cars.
2. **Game Events Data (`Dodgers.events`)**: Contains game day information.

## Steps Involved

### 1. Data Loading
- The project reads raw traffic data using `SparkSession` and loads it into a DataFrame.
```python
traffic = sc.read.text("Dodgers.data")
```

### 2. Data Transformation
Transforming the traffic data to interpret it as CSV with a custom schema.

```python
schema = StructType([StructField("timestamp", StringType()), StructField("number_of_cars", StringType())])
traffic = sc.read.format("csv").schema(schema).load("Dodgers.data")
```

### 3. Data Type Casting and Cleaning
The column containing the number of cars is cast to int type and cleaned.

```python
from pyspark.sql.functions import col
traffic = traffic.withColumn("number_of_cars_i", col("number_of_cars").cast("int")).drop("number_of_cars")
```

### 4. Aggregating Traffic Data
Traffic data is grouped by timestamp, and the total number of cars is calculated.

```python
from pyspark.sql.functions import sum
traffic_by_time = traffic.groupBy("timestamp").agg(sum("number_of_cars_i").alias("no_of_cars_per_time"))
```

### 5. Date and Time Conversion
The timestamp column is converted to proper date and time format.

```python
from pyspark.sql.functions import to_timestamp
df_datetime = traffic_by_time.withColumn("datetime", to_timestamp("timestamp", "MM/dd/yyyy HH:mm"))
```

### 6. Grouping by Date
Traffic data is grouped by date to calculate the total number of cars per day.

```python
df_datetime = df_datetime.withColumn("date", date_format("datetime", "MM/dd/yyyy")).drop("datetime")
df_datetime = df_datetime.groupBy("date").agg(sum("no_of_cars_per_time").alias("cars_per_date"))
```

### 7. Cleaning Negative Values
Any negative car count values are replaced with zero.

```python
from pyspark.sql.functions import col, when
df_datetime_clean = df_datetime.withColumn("cars_per_date", when(col("cars_per_date") < 0, 0).otherwise(col("cars_per_date")))
```

### 8. Combining Traffic Data with Game Events Data
Game event data is loaded and merged with traffic data to create a combined dataset.

```python
games = sc.read.format("csv").option("inferSchema", "false").load("Dodgers.events")
games = games.withColumn("date", to_timestamp("_c0", "MM/dd/yy"))
dailyTrendCombined = df_datetime_clean.join(games, on="date", how="left")
```

### 9. Analyzing Traffic on Match Days vs Regular Days
A new column is created to label each day as a "Match Day" or "Regular Day" based on whether there was a game on that day.

```python
from pyspark.sql.functions import when
dailyTrendCombined = dailyTrendCombined.withColumn("day", when(col("against").isNull(), "Regular Day").otherwise("Match Day"))
```

The average number of cars on match days and regular days is calculated.

```python
from pyspark.sql.functions import avg
avg_score = dailyTrendCombined.groupBy("day").agg(avg("cars_per_date").alias("avg_cars_on"))
```

### Results and Insights
The dataset provides insights into the traffic patterns on both regular days and match days. By aggregating and visualizing the data, we can identify how game days affect traffic flow in the surrounding area.

### Usage Instructions
Ensure PySpark and other dependencies are installed.
Place the traffic data (Dodgers.data) and event data (Dodgers.events) files in the appropriate directory.
Run the code step-by-step in a PySpark environment.
Review the final output, which includes insights on daily vehicle counts and their correlation with game days.

### Future Improvements
Incorporate more detailed event data, such as time of day, to better analyze traffic trends.
Visualize the results using a tool like Power BI or Tableau for enhanced data exploration.

### License
This project is licensed under the MIT License.

This `README.md` structure follows the same style as the examples you've provided. You ca
