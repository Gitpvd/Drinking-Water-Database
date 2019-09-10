

```python
#declare dependencis
 
from flask_pymongo import PyMongo
import pandas as pd
import datetime
import pprint as pp
import pymongo
# Import Scrape libraries
import requests
import urllib.request
import time
from splinter import Browser
from bs4 import BeautifulSoup as bs
from urllib.parse import urljoin
import itertools
```

## Definitions 


```python
def getridoftoprow(df):
    '''Nifty routine to drop top row of double headed table'''
    dft=df.T 
    dft=dft.drop(dft.columns[[0]], axis=1)
    df=dft.T
    return df

def geturlresponse(url):
    '''Get time out, used more than once'''
    #This will get a response within ten seconds or just move on to the next table
    try:
        response= requests.get(url, timeout=10 )
    except requests.exceptions.Timeout:
        pass   
    return response
```

# Initialize the Database with Mongo, 


```python


conn = 'mongodb://localhost:27017'
client = pymongo.MongoClient(conn)
# declare database
db = client.water_db  # implicitly declared mars DB
collection = db.water_db

```

## I decided to not use the chromedriver as it is problematic and slowish, and not necessary for this task


```python
#executable path to driver
# executable_path = {'executable_path': 'chromedriver.exe'}
# browser = Browser('chrome', **executable_path, headless=False)

#Chromedriver really kinds of sucks, it is slow
```

### This is the EPA table for recommened levels of a list of chemicals for Aquatic Life, not drinking water.  So this is about the fish.


```python
epa_url = 'https://www.epa.gov/wqc/national-recommended-water-quality-criteria-aquatic-life-criteria-table'
#browser.visit(epa_url)  #not using this, it will open up Chrome to the selected web page 

#This will get a response within ten seconds or just move on to the next table
#If it returns 200 worko!
geturlresponse(epa_url)
```




    <Response [200]>



## This table shows pollutant levels and has a concatenated CAS number, the definitive identifier of chemicals.  There is a list of over eight hundred thousand maintained by the EPA.  Chemicals are often referred to by different names so it was imperative to have a unique identifier that chemists use.  The CAS number, as represented here is not properly stuctured, but we can still use it.



```python

#Use Pandas to read all the tables on the page.  
epa_tables = pd.read_html(epa_url) 
getridoftoprow(epa_tables[0]).head()
#df_1.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pollutant  (P = Priority Pollutant)</th>
      <th>CAS Number</th>
      <th>Freshwater CMC1  (acute)  (µg/L)</th>
      <th>Freshwater CCC2  (chronic)  (µg/L)</th>
      <th>Saltwater CMC1  (acute)  (µg/L)</th>
      <th>Saltwater CCC2  (chronic)  (µg/L)</th>
      <th>Publication Year</th>
      <th>Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Aesthetic Qualities</td>
      <td>—</td>
      <td>—</td>
      <td>—</td>
      <td>—</td>
      <td>—</td>
      <td>1986</td>
      <td>See Quality Criteria for Water, 1986 ("Gold Bo...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Aldrin (P)</td>
      <td>309002</td>
      <td>3.0</td>
      <td>—</td>
      <td>1.3</td>
      <td>—</td>
      <td>1980</td>
      <td>These criteria are based on the 1980 criteria ...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Alkalinity</td>
      <td>—</td>
      <td>—</td>
      <td>20000</td>
      <td>—</td>
      <td>—</td>
      <td>1986</td>
      <td>The CCC of 20mg/L is a minimum value except wh...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>alpha-Endosulfan (P)</td>
      <td>959988</td>
      <td>0.22</td>
      <td>0.056</td>
      <td>0.034</td>
      <td>0.0087</td>
      <td>1980</td>
      <td>These criteria are based on the 1980 criteria ...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Aluminum pH 5.0 - 10.5</td>
      <td>7429905</td>
      <td>--</td>
      <td>--</td>
      <td>—</td>
      <td>—</td>
      <td>2018</td>
      <td>The criteria is based on the water chemistry d...</td>
    </tr>
  </tbody>
</table>
</div>




```python
human_health = 'https://www.epa.gov/wqc/national-recommended-water-quality-criteria-human-health-criteria-table'
#browser.visit(human_health)
geturlresponse(human_health)
```




    <Response [200]>




```python
human_health_df = pd.read_html(human_health)

 
human_health_df= getridoftoprow(human_health_df[0]) 
human_health_df.loc[:,'Usage']='Water Testing' # add a new column
human_health_df.columns=['Pollutant','CAS','Human Health for the consumption of Water + Organism (µg/L)','Human Health for the consumption of Organism Only (µg/L)','Publication Year','Notes','Usage']
 
human_health_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pollutant</th>
      <th>CAS</th>
      <th>Human Health for the consumption of Water + Organism (µg/L)</th>
      <th>Human Health for the consumption of Organism Only (µg/L)</th>
      <th>Publication Year</th>
      <th>Notes</th>
      <th>Usage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Acrolein (P)</td>
      <td>107028</td>
      <td>3</td>
      <td>400</td>
      <td>2015</td>
      <td>NaN</td>
      <td>Water Testing</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Acrylonitrile (P)</td>
      <td>107131</td>
      <td>0.061</td>
      <td>7.0</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Aldrin (P)</td>
      <td>309002</td>
      <td>0.00000077</td>
      <td>0.00000077</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
    </tr>
    <tr>
      <th>4</th>
      <td>alpha-Hexachlorocyclohexane (HCH) (P)</td>
      <td>319846</td>
      <td>0.00036</td>
      <td>0.00039</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
    </tr>
    <tr>
      <th>5</th>
      <td>alpha-Endosulfan (P)</td>
      <td>959988</td>
      <td>20</td>
      <td>30</td>
      <td>2015</td>
      <td>NaN</td>
      <td>Water Testing</td>
    </tr>
  </tbody>
</table>
</div>



## Frac list of chemicals, identified by CAS number in a padded form with leading zeros.  This is almost certainly the best way to represent CAS numbers, but we are going to undo it with Pandas so that it has the same format as the Water Pollutant table .


```python
frac = 'https://fracfocus.org/chemical-use/what-chemicals-are-used'
geturlresponse(frac)
```




    <Response [200]>




```python
frac_df = pd.read_html(frac)
hydraulic = frac_df[0]
hydraulic.columns = hydraulic.iloc[0]
hydraulic = getridoftoprow(hydraulic)
hydraulic_pure = hydraulic.dropna()

hydraulic_pure.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Chemical  Name</th>
      <th>CAS</th>
      <th>Chemical  Purpose</th>
      <th>Product Function</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Hydrochloric Acid</td>
      <td>007647-01-0</td>
      <td>Helps  dissolve minerals and initiate cracks i...</td>
      <td>Acid</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Glutaraldehyde</td>
      <td>000111-30-8</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Quaternary Ammonium Chloride</td>
      <td>012125-02-9</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Quaternary Ammonium Chloride</td>
      <td>061789-71-1</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tetrakis Hydroxymethyl-Phosphonium Sulfate</td>
      <td>055566-30-8</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
    </tr>
  </tbody>
</table>
</div>



## This table is processed and then a new Column with a massaged CAS number is generated. Also, an additional columns is added in, Usage.  


```python
hydraulic_pure.loc[:,1]=(hydraulic_pure['CAS']).str.lstrip('0').replace('-','',regex=True)  #
hydraulic_pure.loc[:,'Usage']='Fracking' # add a new column
hydraulic_pure.columns=['Chemical Name','Original CAS','Chemical Purpose','Product Function','CAS','Usage']

hydraulic_pure.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Chemical Name</th>
      <th>Original CAS</th>
      <th>Chemical Purpose</th>
      <th>Product Function</th>
      <th>CAS</th>
      <th>Usage</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>Hydrochloric Acid</td>
      <td>007647-01-0</td>
      <td>Helps  dissolve minerals and initiate cracks i...</td>
      <td>Acid</td>
      <td>7647010</td>
      <td>Fracking</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Glutaraldehyde</td>
      <td>000111-30-8</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
      <td>111308</td>
      <td>Fracking</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Quaternary Ammonium Chloride</td>
      <td>012125-02-9</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
      <td>12125029</td>
      <td>Fracking</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Quaternary Ammonium Chloride</td>
      <td>061789-71-1</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
      <td>61789711</td>
      <td>Fracking</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Tetrakis Hydroxymethyl-Phosphonium Sulfate</td>
      <td>055566-30-8</td>
      <td>Eliminates  bacteria in the water that produce...</td>
      <td>Biocide</td>
      <td>55566308</td>
      <td>Fracking</td>
    </tr>
  </tbody>
</table>
</div>



# Now we have two tables, hydraulic_pure and human_health_df which have a common identifier, CAS.  We can do some joining 
## Executing an inner join yields no common chemicals, which is kind of disturbing.  What is signifies is that none of the chemicals used in fracking are tested for in drinking water, not even methanol, CH4.  Of course things can vary, but a no set in this case is signifcant.  So, the generated set here will be an outer join that assures that we have all data merged together, and that is what we will send to MONGODB.


```python
#reference with shorter names
dfh=human_health_df
dff=hydraulic_pure 
df=pd.merge(dfh, dff, on='CAS', how='outer')
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Pollutant</th>
      <th>CAS</th>
      <th>Human Health for the consumption of Water + Organism (µg/L)</th>
      <th>Human Health for the consumption of Organism Only (µg/L)</th>
      <th>Publication Year</th>
      <th>Notes</th>
      <th>Usage_x</th>
      <th>Chemical Name</th>
      <th>Original CAS</th>
      <th>Chemical Purpose</th>
      <th>Product Function</th>
      <th>Usage_y</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Acrolein (P)</td>
      <td>107028</td>
      <td>3</td>
      <td>400</td>
      <td>2015</td>
      <td>NaN</td>
      <td>Water Testing</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Acrylonitrile (P)</td>
      <td>107131</td>
      <td>0.061</td>
      <td>7.0</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Aldrin (P)</td>
      <td>309002</td>
      <td>0.00000077</td>
      <td>0.00000077</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>alpha-Hexachlorocyclohexane (HCH) (P)</td>
      <td>319846</td>
      <td>0.00036</td>
      <td>0.00039</td>
      <td>2015</td>
      <td>This criterion is based on carcinogenicity of ...</td>
      <td>Water Testing</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>alpha-Endosulfan (P)</td>
      <td>959988</td>
      <td>20</td>
      <td>30</td>
      <td>2015</td>
      <td>NaN</td>
      <td>Water Testing</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>



# Step 3 - Passing the data to a database.  This should be easy...let's hope so.  Above, I already have a named collection,in my water_db database.


```python
from pymongo import MongoClient
import pandas as pd
 

data = df.to_dict(orient='records')  # Here's our added param..
collection.drop() #clean things up before adding
collection.insert_many(data)
```




    <pymongo.results.InsertManyResult at 0x1a15fc8f088>



# CONCLUSION and COMMENT
### This process is really straight forward and for us, was made more complicated by the fact that we also had a complex homework assignment to finish, which I, Kevin, gave priority. We could have chosen a topic a bit more straight forward, but we all agreed that whatever we do should have the possiblity of an added value.  In this case, the added value turned out to be that in our preliminary analysis there does not seem to be any testing of commonly used fracking chemicals in drinking water. That should be of considerable interest to people who have a water source above a frack zone.

### Also, the use of MONGODB for this type of project is awesome.  All we had to do at the end was there lines of code, that could have been two...essentially one line of code if we don't clean house before committing. 

## That is efficient.

#### In my opinon, this was a good project, but I think it should have been scheduled a bit differently.



```python

```
