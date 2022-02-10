I made a demo project as a pipeline workflow.

The workflow will stream the gziped JSON file from the these urls  
    - <http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/meta_Musical_Instruments.json.gz>  
    - <http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Musical_Instruments_5.json.gz>

manipulate price field then stored them in the MSSQL database.

Users requirements, system purpose and others have effect on choosing data model granularity. Two design models came to my mind, I designed a model with all tables and I just discuss it in this documentary and implement the second model in code.

The first model designed is a snowflake model, which can be seen in the image below.

![diagram](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/diagram.png?raw=true)


Which includes Dimensions (Dim\_product, Dim\_Category, Dim\_ProductCategory and Dim\_Riviewer) and Facts (Fact\_Review, Fact\_SalesRank, Fact\_AlsoViewed, Fact\_AlsoBought and Fact\_BoughtTogether). In these facts, count is as measure.

The second model designed includes a dimension called meta\_Musical\_Instruments and a fact called reviews\_Musical\_Instruments\_5. In meta\_Musical\_Instruments there is a numeric column dimensions called price.

Since dimensions are formed non-numerical and discrete values I convert the price field to string discrete values.

![price](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/price.png?raw=true)

After designing the model, the following codes were implemented, which I will explain in the next section.

**pipeline**

Pipeline implementation is performed on the Airflow platform, which consists of two stages and is implemented on a three time a day.

![dags](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/dags.png?raw=true)

![pipeline](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/pipeline.png?raw=true)

![graph](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/graph.png?raw=true)

The first step is to call the dim\_product function, which processes the url of a gziped json file called meta\_Musical\_Instruments that contains product information. And the name of primary key field of the table is set. Then processing method is called.

In method processing, first the json file is extracted line by line as a **stream of data**, the value of the price field is converted, and then the first column is checked, which is equivalent to the key field so that it does not exist in the database (**Duplicate Check**). it will be added. This continues until the number of rows of lines\_collected\_temp reaches 200 and then the bulk\_insert function is called, whose job is to insert lines\_collected\_temp records into the MSSQL database.I Used a SSPI Authentication for the Connection String to DB  considering more secure CS. In bulk\_insert method, the input is entered as chunk in the database. The size of this task is considered 100. I used this approach considering that bulk operation are more optimized for such scenarios and the numbers are for just demonstration they should be tuned in production based on hardware IOPS capacity.

![dimension](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/dimension.png?raw=true)

This table does not include other sections also\_viewed, also\_bought, etc., each of which can be implemented as a fact.

In the second step, the file is checked, which is known as a fact. The steps are very similar to the previous section, and the same methods are called only with different inputs.

![fact](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/fact.png?raw=true)

In bulk\_insert method, data and table name are received and in chunk size, 100 pieces of information are entered in MSSQL database.

In the load\_ids method, the table name and its key are received and the ids stored in the database are fetched. This list is used to prevent duplicate insertion. I did this to speed up the process instead of connecting to sql for every single row, in production a key-value database like Memcached or REDIS should be implemented in clusters for this job.

The logger\_handler method is also written to manage logs. I’m using a file handler here but as its implemented a specific file and logging transparent from the rest of the code, it could easily adapt to a **ELK** stack or **Prometheus** in production.

![logs](https://github.com/ElnazGhasemi/AmazonDataModeling/blob/main/Images/logs.png?raw=true)

To Do Comments:

- The internet connection may be lost. **Kafka** server can be used to resume work after reconnection. In this case, a **producer** must be defined to receive data and define a **consumer** to given data.
- Can make a more structured code.
