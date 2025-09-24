# 04-ParallelComputation

# Parallel Computation

Python과 수행하는 병렬처리 방법

## Parallel computers
- Multiprocessor/multicore: 여러 개의 프로세서들이 공유되는 메모리에서 데이터를 처리
- Cluster: 여러 개의 프로세서/메모리 유닛(units)들이 네트워크 위에서 데이터를 교환함으로써 처리를 함께 수행함
- Co-processor: general-purpose 프로세서는 GPU와 같은 special-purpose에 특정 작업을 위임(delegates)

## Parallel Programming
- 전체 tasks을 독립적인 subtasks로 분해(decomposition) 그리고 그들간의 데이터 흐름을 정의
- 프로세서들 위에 subtasks들을 배분하여 전체실행시간(total execution time)을 최소화함
- For clusters: communication time을 최소화하기 위해 노드(nodes) 위의 데이터의 적절한 배분이 필요
- For multiprocessors: 대기 시간(waiting times)을 최소화하기 위해 메모리 접근 패턴을 최적화함
- 개별 프로세스간의 동기화(Synchronization)

## MapReduce

~~~python
from time import sleep
def f(x):
    sleep(1)
    return x*x
L = list(range(8))
L

~~~

`%time` 명령어를 이용하여 코드 셸의 수행시간을 측정함

~~~python
%time sum(f(x) for x in L)

~~~

~~~python
%time sum(map(f,L))

~~~

## Multiprocessing 

`multiprocessing` 라이브러리는 spawning processes를 지원함
우리는 컴퓨터 내 우리가 launch할 수 있는 병렬 프로세스( concurrent processes )의 개수를 보여줄 수 있음
We can use it to display how many concurrent processes you can launch on your computer.

~~~python
from multiprocessing import cpu_count

cpu_count()

~~~

## Futures

`concurrent.futures` 모듈은 callable 객체를 비동기적(asynchronously)으로 실행할 수 있는 high-level interface이다

비동기 실행(asynchronous execution)은 다음 개체와 함께 수행된다:
- ThreadPoolExecutor을 사용하여 **threads**
- ProcessPoolExecutor를 사용하여 **processes**를 분리(separate)
위의 둘다 동일한 인터페이스를 구현하고, 이것은 추상화된 Executor 클래스에 정의된다.

`concurrent.futures` 는 windows os 위에서는 **processes**를 시작할 수 없다. 따라서, window 사용자는 다음 라이브러리를 필수로 설치해야한다.
[loky](https://github.com/tomMoral/loky).

`%%file`을 사용하여 파일을 생성할 수있다

~~~python
%%file pmap.py
from concurrent.futures import ProcessPoolExecutor
from time import sleep, time

def f(x):
    sleep(1)
    return x*x

L = list(range(8))

if __name__ == '__main__':
    
    begin = time()
    with ProcessPoolExecutor() as pool:

        result = sum(pool.map(f, L))
    end = time()
    
    print(f"result = {result} and time = {end-begin}")

~~~

~~~python
import sys
!{sys.executable} pmap.py

~~~

세부적인 메소드의 역할은 다음과 같다.
- `ProcessPoolExecutor` 컴퓨터 내 물리적인 core당 하나의 slave process를 시작및수행(launch)한다.
- `pool.map` 입력 리스트를 여러개의 chunks로 나눈뒤에 queue에 (function + chunk)의 tasks을 넣는다.
- 각 slave process는 (function + chunk) 작업을 취한 뒤에, map(function, chunk)을 수행하고, 그리고 results list에 결과를 넣는다.
- master process 위의 `pool.map` 모든 tasks들이 완료 질때까지 기다리고, result lists의 종합한(concatenation) 결과를 반환한다.

~~~python
%%time
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor() as pool:

    results = sum(pool.map(f, L))
    
print(results)

~~~

## Thread and Process: 차이점

- A **process** is an instance of a running program. 
- **Process** may contain one or more **threads**, but a **thread** cannot contain a **process**.
- **Process** has a self-contained execution environment. It has its own memory space. 
- Application running on your computer may be a set of cooperating **processes**.
- **Process** don't share its memory, communication between **processes** implies data serialization.

- A **thread** is made of and exist within a **process**; every **process** has at least one **thread**. 
- Multiple **threads** in a **process** share resources, which helps in efficient communication between **threads**.
- **Threads** can be concurrent on a multi-core system, with every core executing the separate **threads** simultaneously.

## The Global Interpreter Lock (GIL)

- The Python interpreter is not thread safe.
- A few critical internal data structures may only be accessed by one thread at a time. Access to them is protected by the GIL.
- Attempts at removing the GIL from Python have failed until now. The main difficulty is maintaining the C API for extension modules.
- Multiprocessing avoids the GIL by having separate processes which each have an independent copy of the interpreter data structures.
- The price to pay: serialization of tasks, arguments, and results.

## Parallelize text files downloads

- Victor Hugo http://www.gutenberg.org/files/135/135-0.txt
- Marcel Proust http://www.gutenberg.org/files/7178/7178-8.txt
- Emile Zola http://www.gutenberg.org/files/1069/1069-0.txt
- Stendhal http://www.gutenberg.org/files/44747/44747-0.txt

~~~python
%mkdir books

~~~

~~~python
%%time
import urllib.request as url
source = "https://mmassd.github.io/"  # "http://svmass2.mass.uhb.fr/hub/static/datasets/"
url.urlretrieve(source+"books/hugo.txt",     filename="books/hugo.txt")
url.urlretrieve(source+"books/proust.txt",   filename="books/proust.txt")
url.urlretrieve(source+"books/zola.txt",     filename="books/zola.txt")
url.urlretrieve(source+"books/stendhal.txt", filename="books/stendhal.txt")

~~~

### Exercise 4.1

Use `ThreadPoolExecutor` to parallelize the code above.

## Wordcount
아래 함수(mapper, partitioner, reducer)는 single core에 수행되는 프로그램이다.

~~~python
from glob import glob
from collections import defaultdict
from operator import itemgetter
from itertools import chain
from concurrent.futures import ThreadPoolExecutor

def mapper(filename):
    " split text to list of key/value pairs (word,1)"

    with open(filename) as f:
        data = f.read()
        
    data = data.strip().replace(".","").lower().split()
        
    return sorted([(w,1) for w in data])

def partitioner(mapped_values):
    """ get lists from mapper and create a dict with
    (word,[1,1,1])"""
    
    res = defaultdict(list)
    for w, c in mapped_values:
        res[w].append(c)
        
    return res.items()

def reducer( item ):
    """ Compute words occurences from dict computed
    by partioner
    """
    w, v = item
    return (w,len(v))

~~~

## Parallel map


- 다음 `mapper` 함수를 현재 프로세스 이름을 함수 내 출력해보도록 하자.

~~~python
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor

def process_name(n):
    " prints out the current process name "
    print(f"{mp.current_process().name} ")

with ProcessPoolExecutor() as e:
    _ = e.map(process_name, range(mp.cpu_count()))

~~~

### Exercise 4.2

- `mapper` 함수에 위의 `print`를 추가하여 mapper 함수를 향상시키도록 하자.

## Parallel reduce

- 병렬 reduce 연산을 위해, 데이터는 컨테이너 내 정렬되어 있어야만 한다. 예로 key 값 등으로 정렬되어 있다. `partitioner` function는 컨테이너를 반환한다.

### Exercise 4.3

`ThreadPoolExecutor`를 사용하여 three functions을 향상한 parallel 프로그램을 작성하시오. 프로그램은 모든 "books\*.txt"을 읽어 병렬적으로 map과 reduce를 수행한다.

## Crawling Webpage at parallel

우리는 열거된 특정 정보(예로, 쇼핑 아이템, 책)을 하나 이상의 사이트로부터 지속적으로 데이터 수집하길 원한다. 이를 위해, `BeautifulSoup` 과 같은 라이브러리를 자주 사용하지만, 기본적으로 built-in parallel 처리를 지원하지 않는다. 우리는 기존의 webpage crawling을 병렬처리하도록 개선해보고자 한다.

- 주의: 종종 일반 PC에서는 proxy 로 인해 정상적인 동작이 되지 않을 수 있습니다.

### 1. Getting the data

- [The Latin Library](http://www.thelatinlibrary.com/) 는 무료로 접근 가능한 Latin text의 대규모 데이터를 가지고 있다.

~~~python
from bs4 import BeautifulSoup  # web scraping library
from urllib.request import *

base_url = "http://www.thelatinlibrary.com/"
home_content = urlopen(base_url)

soup = BeautifulSoup(home_content, "lxml")
author_page_links = soup.find_all("a")
author_pages = [ap["href"] for i, ap in enumerate(author_page_links) if i < 49]

~~~

~~~python
author_pages[:5]

~~~

### 2. Generate html links

- Latin texts를 가리키는 모든 links의 리스트를 생성한다. 여기서 Latin Library는 구조화된 포맷과 함께 링크를 구성하었기 때문에 저자 이름을 통해 링크를 탐지한다.

~~~python
ap_content = list()
for ap in author_pages:
    ap_content.append(urlopen(base_url + ap))

book_links = list()
for path, content in zip(author_pages, ap_content):
    author_name = path.split(".")[0]
    ap_soup = BeautifulSoup(content, "lxml")
    book_links += ([link for link in ap_soup.find_all("a", {"href": True}) if author_name in link["href"]])

~~~

~~~python
book_links[:5]

~~~

### 3. Download webpages content

일부(100개)를 가져와서 `book-{03d}.dat` 파일이름 형식으로 데이터로 저장한다.

~~~python
from urllib.error import HTTPError

num_pages = 100

for i, bl in enumerate(book_links[:num_pages]):
    print("Getting content " + str(i + 1) + " of " + str(num_pages), end="\r", flush=True)
    try:
        content = urlopen(base_url + bl["href"]).read()
        with open(f"book-{i:03d}.dat","wb") as f:
            f.write(content)
    except HTTPError as err:
        print("Unable to retrieve " + bl["href"] + ".")
        continue

~~~

### 4. Read data files

glob를 사용하여 특정 파일이름 패턴을 가진 파일을 명시한다. 그리고 각 파일을 읽어서 데이터를 text에 모두 붙인다.

~~~python
from glob import glob
files = glob('book*.dat')
texts = list()
for file in files:
    with open(file,'rb') as f:
        text = f.read()
    texts.append(text)

~~~

### 5. Extract the text from html and split the text at periods to convert it into sentences.

html로부터 text를 추출해내고,마침표를 사용하여 문장(sentences) 단위로 해당 정보를 분할합니다.

~~~python
%%time
from bs4 import BeautifulSoup

sentences = list()

for i, text in enumerate(texts):
    print("Document " + str(i + 1) + " of " + str(len(texts)), end="\r", flush=True)
    textSoup = BeautifulSoup(text, "lxml")
    paragraphs = textSoup.find_all("p", attrs={"class":None})
    prepared = ("".join([p.text.strip().lower() for p in paragraphs[1:-1]]))
    for t in prepared.split("."):
        part = "".join([c for c in t if c.isalpha() or c.isspace()])
        sentences.append(part.strip())

# print first and last sentence to check the results
print(sentences[0])
print(sentences[-1])

~~~

### Exercise 4.4

`concurrent.futures`를 사용하여 마지막 함수를 병렬 처리가 가능하도록 재작성하시오

### Exercise 4.5
언급한 함수들 중에 병렬처리 (map /reduce) 가 가능한 부분이 있다면, 자유롭게  `concurrent.futures`를 사용하여 병렬처리가 가능하도록 재작성하시오.

## References

- [Using Conditional Random Fields and Python for Latin word segmentation](https://medium.com/@felixmohr/using-python-and-conditional-random-fields-for-latin-word-segmentation-416ca7a9e513)
