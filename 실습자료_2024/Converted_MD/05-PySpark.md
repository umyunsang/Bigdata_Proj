# 05-PySpark

# PySpark

![Logo](images/apache_spark_logo.png)

- [Apache Spark](https://spark.apache.org)는 2014년에 첫 릴리즈됨
- [Matei Zaharia](http://people.csail.mit.edu/matei)의 수업과제로 처음 시작되었음. 이후, 박사학위논문으로 제시되었음.
- Spark [Scala](https://www.scala-lang.org)는 Scala언어로 작성되었음
- 이미지 크레딧: [Databricks](https://databricks.com/product/getting-started-guide).

- Apache Spark는 빠르고, 일반 목적(general-purpose)인 클러스터 컴퓨팅 시스템임.
- Java, Scala, Python 에서 고수준의 API를 제공하고, 일반적인 실행 그래프를 지원하는 최적화된 엔진임
- Spark는 `map`, `filter`, `groupby`, `join`의 단위 연산자와 함께 "big data" 콜렉션를 다룰 수 있음.  이러한 일반적인 패턴과 함께, 사용자들은 복잡한 구조를 가진 계산을 다룰 수 있으며, 해당 계산 또한 구조화됨.
- SQL을 위한 [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) 의 고수준 API를 지원하고 구조화된 데이터 처리가 가능함. 머신러닝을 위한 [MLlib](https://spark.apache.org/docs/latest/ml-guide.html), 그래프 처리를 위한 [GraphX](https://spark.apache.org/docs/latest/graphx-programming-guide.html), 그리고 구조화된 스트림(Structured Streaming)을 지원함

## Resilient distributed datasets (RDD)

- Apache Spark의 기본적인 추상화인 RDD는 읽기 전용(read-only), 병렬(parallel), 분산(distributed), 오류감내(fault-tolerent)의 콜렉션(Collection)임
- 사용자는 새로운 RDD를 생성하기 위해 반복적으로 콜렉션의 모든 아이템에 함수를 벙렬처리형태로 적용할 수 있다.
- 해당 데이터는 컴퓨터의 클러스터 내 노드간에 분산되어진다.
- Spark 내 구현된 함수는 콜렉션의 요소들간의 병렬처리형태로 동작된다.
- Spark 프레임워크는 프로그래머의 간섭없이 데이터와 처리를 다른 노드에 할당한다.
- RDD는 기계 오류위에서 자동으로 재생성된다.

## Spark 프로그램(Program)의 생명주기(Lifecycle)

1. 외부데이터로부터 입력 RDD를 생성하거나 드라이버 프로그램 내 콜렉션을 병렬화함
2. `filter()` 혹은 `map()`과 같은 transofmrations을 사용하여 새로운 RDD를 정의하기 위해 transform함
3. 재사용될 필요가 있는 중간단계(Intermediate) RDD를 위해 Spark에 `cache()`를 요청
4. 병렬 계산을 수행하기 위해 `count()` 혹은 `collect()`과 같은 action을 수행. Spark에 의헤 해당 계산은 최적화됨.

## 분산 데이터 위에서의 연산(Operations)

- 두 종류의 연산: **transformations**과 **actions**
- Transformations은 *lazy*, 즉시(immediately) 계산되지 않는다.
- Action이 수행될때, Transformations이 실행된다.

## [Transformations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#transformations) (lazy)
다음은 transformation에 해당되는 함수리스트 입니다.
```
map() flatMap()
filter() 
mapPartitions() mapPartitionsWithIndex() 
sample()
union() intersection() distinct()
groupBy() groupByKey()
reduceBy() reduceByKey()
sortBy() sortByKey()
join()
cogroup()
cartesian()
pipe()
coalesce()
repartition()
partitionBy()
...
```

## [Actions](https://spark.apache.org/docs/latest/rdd-programming-guide.html#actions)
다음은 actions에 해당되는 함수리스트 입니다.
```
reduce()
collect()
count()
first()
take()
takeSample()
saveToCassandra()
takeOrdered()
saveAsTextFile()
saveAsSequenceFile()
saveAsObjectFile()
countByKey()
foreach()
```

## Python API

PySpark 는 Py4J를 사용하며, Java objecs를 동적으로 접근하는 Python 프로그램을 가능하게 함

![PySpark Internals](images/YlI8AqEl.png)

## The `SparkContext` class

- Apche Spark와 작업하려고할때, 사용자는 `pyspark.SparkContext` 컨텍스트(context)의 인스턴스 위에서 메소드를 호출함.

- 전형적으로, 이 객체(object)의 인스턴스는 자동으로 생성되고, 변수 `sc`에 할당됨.

- `SparkContext` 내 `parallelize` 메소드는 기본 Python 콜렉션 을 RDD로 변환하도록 함; 기본적으로 우리는 대용량 파일이나, HBase table로부터 RDD를 생성함

## First example

PySpark는 기본적으로 `sys.path`에 위에 있지 않으며, 그것이 기본 라이브러리를 사용하지 못할 수 도 있음. 따라서, site-package에 pyspark를  symlinking하거나 런타임에 pyspark를 sys.path에 추가하여 이부분을 해결하고자 함. [findspark](https://github.com/minrk/findspark)

~~~python
import os, sys
sys.executable

~~~

~~~python
#os.environ["SPARK_HOME"] = "/opt/spark-3.0.1-bin-hadoop2.7"
os.environ["PYSPARK_PYTHON"] = sys.executable

~~~

~~~python
import pyspark

sc = pyspark.SparkContext(master="local[*]", appName="FirstExample")
sc.setLogLevel("ERROR")

~~~

~~~python
print(sc) # it is like a Pool Processor executor

~~~

## Create your first RDD

~~~python
data = list(range(8))
rdd = sc.parallelize(data) # create collection
rdd

~~~

### Exercise

`faker` 패키지와 함께 파일 `sample.txt` 생성하시오. Read and load it into a RDD with the `textFile` spark 함수와 함께 파일을 읽고 RDD 안으로 그것을 로드(Load)하시오.

여기서 faker는 임의텍스트를 생성합니다. 그 밖에, name, address, profile 등을 생성가능하며, 한국어 자료 생성을 위해 인스턴스 생성시 매개변서 `ko_KR`와 함께 시작하시면 됩니다.

~~~python
from faker import Faker
fake = Faker()
Faker.seed(0)

with open("sample.txt","w") as f:
    f.write(fake.text(max_nb_chars=1000))
    
rdd = sc.textFile("sample.txt")

~~~

### Collect

Action / To Driver: Return all items in the RDD to the driver in a single list

![](images/DUO6ygB.png)

Source: https://i.imgur.com/DUO6ygB.png

### Exercise 

Collect the text you read before from the `sample.txt`file.

### Map

Transformation / Narrow: Return a new RDD by applying a function to each element of this RDD

![](images/PxNJf0U.png)

Source: http://i.imgur.com/PxNJf0U.png

~~~python
rdd = sc.parallelize(list(range(8)))
rdd.map(lambda x: x ** 2).collect() # Square each element

~~~

### Exercise

Replace the lambda function by a function that contains a pause (sleep(1)) and check if the `map` operation is parallelized.

### Filter

Transformation / Narrow: Return a new RDD containing only the elements that satisfy a predicate

![](images/GFyji4U.png)
Source: http://i.imgur.com/GFyji4U.png

~~~python
# Select only the even elements
rdd.filter(lambda x: x % 2 == 0).collect()

~~~

### FlatMap

Transformation / Narrow: Return a new RDD by first applying a function to all elements of this RDD, and then flattening the results

![](images/TsSUex8.png)

~~~python
rdd = sc.parallelize([1,2,3])
rdd.flatMap(lambda x: (x, x*100, 42)).collect()

~~~

### Exercise

Use FlatMap to clean the text from `sample.txt`file. Lower, remove dots and split into words.

### GroupBy

Transformation / Wide: Group the data in the original RDD. Create pairs where the key is the output of a user function, and the value is all items for which the function yields this key.

![](images/gdj0Ey8.png)

~~~python
rdd = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
rdd = rdd.groupBy(lambda w: w[0])
[(k, list(v)) for (k, v) in rdd.collect()]

~~~

### GroupByKey

Transformation / Wide: Group the values for each key in the original RDD. Create a new pair where the original key corresponds to this collected group of values.

![](images/TlWRGr2.png)

~~~python
rdd = sc.parallelize([('B',5),('B',4),('A',3),('A',2),('A',1)])
rdd = rdd.groupByKey()
[(j[0], list(j[1])) for j in rdd.collect()]

~~~

### Join

Transformation / Wide: Return a new RDD containing all pairs of elements having the same key in the original RDDs

![](images/YXL42Nl.png)

~~~python
x = sc.parallelize([("a", 1), ("b", 2)])
y = sc.parallelize([("a", 3), ("a", 4), ("b", 5)])
x.join(y).collect()

~~~

### Distinct

Transformation / Wide: Return a new RDD containing distinct items from the original RDD (omitting all duplicates)

![](images/Vqgy2a4.png)

~~~python
rdd = sc.parallelize([1,2,3,3,4])
rdd.distinct().collect()

~~~

### KeyBy

Transformation / Narrow: Create a Pair RDD, forming one pair for each item in the original RDD. The pair’s key is calculated from the value via a user-supplied function.

![](images/nqYhDW5.png)

~~~python
rdd = sc.parallelize(['John', 'Fred', 'Anna', 'James'])
rdd.keyBy(lambda w: w[0]).collect()

~~~

## Actions

### Map-Reduce operation 

Action / To Driver: Aggregate all the elements of the RDD by applying a user function pairwise to elements and partial results, and return a result to the driver

![](images/R72uzwX.png)

~~~python
from operator import add
rdd = sc.parallelize(list(range(8)))
rdd.map(lambda x: x ** 2).reduce(add) # reduce is an action!

~~~

### Max, Min, Sum, Mean, Variance, Stdev

Action / To Driver: Compute the respective function (maximum value, minimum value, sum, mean, variance, or standard deviation) from a numeric RDD

![](images/HUCtib1.png)

### CountByKey

Action / To Driver: Return a map of keys and counts of their occurrences in the RDD

![](images/jvQTGv6.png)

~~~python
rdd = sc.parallelize([('J', 'James'), ('F','Fred'), 
                    ('A','Anna'), ('J','John')])

rdd.countByKey()

~~~

~~~python
# Stop the local spark cluster
sc.stop()

~~~

### Exercise 10.1 Word-count in Apache Spark

- Write the sample text file

- Create the rdd with `SparkContext.textFile method`
- lower, remove dots and split using `rdd.flatMap`
- use `rdd.map` to create the list of key/value pair (word, 1)
- `rdd.reduceByKey` to get all occurences
- `rdd.takeOrdered`to get sorted frequencies of words

All documentation is available [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html?highlight=textfile#pyspark.SparkContext) for textFile and [here](https://spark.apache.org/docs/2.1.0/api/python/pyspark.html?highlight=textfile#pyspark.RDD) for RDD. 

For a global overview see the Transformations section of the [programming guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)


## SparkSession

Since SPARK 2.0.0,  SparkSession provides a single point 
of entry to interact with Spark functionality and
allows programming Spark with DataFrame and Dataset APIs. 

###  $\pi$ computation example

- We can estimate an approximate value for $\pi$ using the following Monte-Carlo method:

1.    Inscribe a circle in a square
2.    Randomly generate points in the square
3.    Determine the number of points in the square that are also in the circle
4.    Let $r$ be the number of points in the circle divided by the number of points in the square, then $\pi \approx 4 r$.
    
- Note that the more points generated, the better the approximation

See [this tutorial](https://computing.llnl.gov/tutorials/parallel_comp/#ExamplesPI).


### Exercise 9.2

Using the same method than the PI computation example, compute the integral
$$
I = \int_0^1 \exp(-x^2) dx
$$
You can check your result with numpy

~~~python
# numpy evaluates solution using numeric computation. 
# It uses discrete values of the function
import numpy as np
x = np.linspace(0,1,1000)
np.trapz(np.exp(-x*x),x)

~~~

numpy and scipy evaluates solution using numeric computation. It uses discrete values of the function

~~~python
import numpy as np
from scipy.integrate import quad
quad(lambda x: np.exp(-x*x), 0, 1)
# note: the solution returned is complex 

~~~

### Correlation between daily stock

- Data preparation

~~~python
import os  # library to get directory and file paths
import tarfile # this module makes possible to read and write tar archives

def extract_data(name, where):
    datadir = os.path.join(where,name)
    if not os.path.exists(datadir):
       print("Extracting data...")
       tar_path = os.path.join(where, name+'.tgz')
       with tarfile.open(tar_path, mode='r:gz') as data:
          data.extractall(where)
            
extract_data('daily-stock','data') # this function call will extract json files

~~~

~~~python
import json
import pandas as pd
import os, glob

here = os.getcwd()
datadir = os.path.join(here,'data','daily-stock')
filenames = sorted(glob.glob(os.path.join(datadir, '*.json')))
filenames

~~~

~~~python
%rm data/daily-stock/*.h5

~~~

~~~python
from glob import glob
import os, json
import pandas as pd

for fn in filenames:
    with open(fn) as f:
        data = [json.loads(line) for line in f]
        
    df = pd.DataFrame(data)
    
    out_filename = fn[:-5] + '.h5'
    df.to_hdf(out_filename, '/data')
    print("Finished : %s" % out_filename.split(os.path.sep)[-1])

filenames = sorted(glob(os.path.join('data', 'daily-stock', '*.h5')))  # data/json/*.json

~~~

### Sequential code

~~~python
filenames

~~~

~~~python
with pd.HDFStore('data/daily-stock/aet.h5') as hdf:
    # This prints a list of all group names:
    print(hdf.keys())

~~~

~~~python
df_test = pd.read_hdf('data/daily-stock/aet.h5')

~~~

~~~python
%%time

series = []
for fn in filenames:   # Simple map over filenames
    series.append(pd.read_hdf(fn)["close"])

results = []

for a in series:    # Doubly nested loop over the same collection
    for b in series:  
        if not (a == b).all():     # Filter out comparisons of the same series 
            results.append(a.corr(b))  # Apply function

result = max(results)
result

~~~

### Exercise 9.3

Parallelize the code above with Apache Spark.

- Change the filenames because of the Hadoop environment.

~~~python
import os, glob

here = os.getcwd()
filenames = sorted(glob.glob(os.path.join(here,'data', 'daily-stock', '*.h5')))
filenames

~~~

If it is not started don't forget the PySpark context

Computation time is slower because there is a lot of setup, workers creation, there is a lot of communications the correlation function is too small

### Exercise 9.4 Fasta file example

Use a RDD to calculate the GC content of fasta file nucleotide-sample.txt:

$$\frac{G+C}{A+T+G+C}\times100 \% $$

Create a rdd from fasta file genome.txt in data directory and count 'G' and 'C' then divide by the total number of bases.

### Another example

Compute the most frequent sequence with 5 bases.
