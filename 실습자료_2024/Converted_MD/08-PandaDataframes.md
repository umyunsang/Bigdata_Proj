# 08-PandaDataframes

# Pandas Dataframes

~~~python
%matplotlib inline
%config InlineBackend.figure_format = 'retina'
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

pd.set_option("display.max_rows", 8)
plt.rcParams['figure.figsize'] = (9, 6)

~~~

## Create a [DataFrame](https://pandas.pydata.org/pandas-docs/stable/dsintro.html#dataframe)

~~~python
dates = pd.date_range('20130101', periods=6)
pd.DataFrame(np.random.randn(6,4), index=dates, columns=list('ABCD'))

~~~

~~~python
pd.DataFrame({'A' : 1.,
              'B' : pd.Timestamp('20130102'),
              'C' : pd.Series(1,index=list(range(4)),dtype='float32'),
              'D' : np.arange(4,dtype='int32'),
              'E' : pd.Categorical(["test","train","test","train"]),
              'F' : 'foo' })

~~~

## Load Data from CSV File

~~~python
url = "https://www.fun-mooc.fr/c4x/agrocampusouest/40001S03/asset/AnaDo_JeuDonnees_TemperatFrance.csv"
french_cities = pd.read_csv(url, delimiter=";", encoding="latin1", index_col=0)
french_cities

~~~

## Viewing Data

~~~python
french_cities.head()

~~~

~~~python
french_cities.tail()

~~~

## Index

~~~python
french_cities.index

~~~

We can rename an index by setting its name.

~~~python
french_cities.index.name = "City"
french_cities.head()

~~~

~~~python
import locale
import calendar
 
locale.setlocale(locale.LC_ALL,'C')
 
months = calendar.month_abbr
print(*months)
 
french_cities.rename(
  columns={ old : new 
           for old, new in zip(french_cities.columns[:12], months[1:])
          if old != new },
  inplace=True)
 
french_cities.rename(columns={'Moye':'Mean'}, inplace=True)
french_cities

~~~

### Exercise: Rename DataFrame Months in English

## From a local or remote HTML file
We can download and extract data about mean sea level stations around the world from the [PSMSL website](http://www.psmsl.org/).

~~~python
# Needs `lxml`, `beautifulSoup4` and `html5lib` python packages
table_list = pd.read_html("http://www.psmsl.org/data/obtaining/")

~~~

~~~python
# there is 1 table on that page which contains metadata about the stations where 
# sea levels are recorded
local_sea_level_stations = table_list[0]
local_sea_level_stations

~~~

## Indexing on DataFrames

~~~python
french_cities['Lati']  # DF [] accesses columns (Series)

~~~

`.loc` and `.iloc` allow to access individual values, slices or masked selections:

~~~python
french_cities.loc['Rennes', "Sep"]

~~~

~~~python
french_cities.loc['Rennes', ["Sep", "Dec"]]

~~~

~~~python
french_cities.loc['Rennes', "Sep":"Dec"]

~~~

## Masking

~~~python
mask = [True, False] * 6 + 5 * [False]
print(french_cities.iloc[:, mask])

~~~

~~~python
print(french_cities.loc["Rennes", mask])

~~~

## New column

~~~python
french_cities["std"] = french_cities.iloc[:,:12].std(axis=1)
french_cities

~~~

~~~python
french_cities = french_cities.drop("std", axis=1) # remove this new column

~~~

~~~python
french_cities

~~~

## Modifying a dataframe with multiple indexing

~~~python
# french_cities['Rennes']['Sep'] = 25 # It does not works and breaks the DataFrame
french_cities.loc['Rennes']['Sep'] # = 25 is the right way to do it

~~~

~~~python
french_cities

~~~

## Transforming datasets

~~~python
french_cities['Mean'].min(), french_cities['Ampl'].max()

~~~

## Apply

Let's convert the temperature mean from Celsius to Fahrenheit degree.

~~~python
fahrenheit = lambda T: T*9/5+32
french_cities['Mean'].apply(fahrenheit)

~~~

## Sort

~~~python
french_cities.sort_values(by='Lati')

~~~

~~~python
french_cities = french_cities.sort_values(by='Lati',ascending=False)
french_cities

~~~

## Stack and unstack

Instead of seeing the months along the axis 1, and the cities along the axis 0, let's try to convert these into an outer and an inner axis along only 1 time dimension.

~~~python
pd.set_option("display.max_rows", 20)
unstacked = french_cities.iloc[:,:12].unstack()
unstacked

~~~

~~~python
type(unstacked)

~~~

## Transpose

The result is grouped in the wrong order since it sorts first the axis that was unstacked. We need to transpose the dataframe.

~~~python
city_temp = french_cities.iloc[:,:12].transpose()
city_temp.plot()

~~~

~~~python
city_temp.boxplot(rot=90);

~~~

## Describing

~~~python
french_cities['Région'].describe()

~~~

~~~python
french_cities['Région'].unique()

~~~

~~~python
french_cities['Région'].value_counts()

~~~

~~~python
# To save memory, we can convert it to a categorical column:
french_cities["Région"] = french_cities["Région"].astype("category")

~~~

~~~python
french_cities.memory_usage()

~~~

## Data Aggregation/summarization

## groupby

~~~python
fc_grouped_region = french_cities.groupby("Région")
type(fc_grouped_region)

~~~

~~~python
for group_name, subdf in fc_grouped_region:
    print(group_name)
    print(subdf)
    print("")

~~~

### Exercise

Consider the following dataset [UCI Machine Learning Repository Combined Cycle Power Plant Data Set](https://archive.ics.uci.edu/ml/datasets/Combined+Cycle+Power+Plant).
This dataset consists of records of measurements relating to peaker power plants of 10000 points over 6 years (2006-2011).

**Variables**
- AT = Atmospheric Temperature in C
- V = Exhaust Vaccum Speed
- AP = Atmospheric Pressure
- RH = Relative Humidity
- PE = Power Output

We want to model the power output as a function of the other parameters.

Observations are in 5 excel sheets of about 10000 records in "Folds5x2_pp.xlsx". These 5 sheets are same data shuffled.
- Read this file with the pandas function [read_excel](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html). What is the type returned by this function?
- Implement a `select` function to regroup all observations in a pandas serie.
- Use `select` function and `corr` to compute the maximum correlation.
- Parallelize this loop with `concurrent.futures`.
