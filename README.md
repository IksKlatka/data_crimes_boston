A project about incidents in Boston from 2015 to 2022. 

Initially the project was supposed to be pure data analysis, 
but over time I found that I would expand it to include a CRUD system for the PostgreSQL database and a CRUD API. 

Folders are named according to their purpose. 

The first task was to write algorithms that cleaned up the data in such a way 
that it was optimal for each year in the collection. 
With these, I filled in a lot of missing data by filling in based on averages and medians. 
Duplicates were removed and data types replaced with correct ones. 

Data analysis consisted of creating graphs with the Matplotlib library, 
thus answering interesting questions about the data. 

Using the Alembic library, I created database migration scripts and
then I created a system to manage this data in the database. This system is asynchronous, 
which allowed me to send all the data to the database faster, and will be helpful when sending multiple queries. 
It is based on the 'databases' and SQLAlchemy libraries. 

I will create a CRUD API using FASTAPI, in which I will use the functions that exist in the CRUD system. 
I hope to extend the API with more creative queries than the basic ones included in the system. 

Data for this project comes from kaggle, provided by Analyze Boston:
https://www.kaggle.com/datasets/AnalyzeBoston/crimes-in-boston



