# 01-WordCount

# Wordcount

- [Wikipedia](https://en.wikipedia.org/wiki/Word_count)

- Word count 예제는 텍스트 파일을 읽어서 발생한 단어의 갯수를 세는 것임.
- Word count 는 일반적으로 번역기translators)에 의해 사용되고, 번역작업에 대한 비용을 결정한다.
- 빅데이터 프로그래밍에서 'Hello world' 프로그램 개발과 동일하다

가이드라인:
- *구글링, ChatGPT 사용은 최소화, 교수자 혹은 보조연구원에게 물어보도록 할것, 또는 python help function을 사용해 볼것*
- *우선 대략적으로 주석이나 간단한 pseudo-code를 시도하고 그다음에 최적화된 코드를 생각하시오.*
- *다른 학생으로부터 코드를 절대 받지 말기 바랍니다*
- *Exercise의 답은 LMS를 통해 게시됩니다.*

## Create sample text file

~~~python
from lorem import text

with open("sample.txt", "w") as f:
    for i in range(2):
        f.write(text())

~~~

### Exercise 1.1
sample.txt 파일 내의 고유한 단어(word)의 갯수와 문자의 갯수를 세는 프로그램을 계산하시오
Write a python program that counts the number of lines, different words and characters in that file.

~~~python
%%bash
wc sample.txt
du -h sample.txt

~~~

### Exercise 1.2

map_words의 이름을 가진 함수를 생성하시오. 해당 함수는 파일이름(filename)을 입력으로 받아 모든 단어를 반환한다.

```pytb
map_words("sample.txt")[:5] # first five words
['adipisci', 'adipisci', 'adipisci', 'adipisci', 'adipisci']
```

## Sorting a dictionary by value

By default, if you use `sorted` function on a `dict`, it will use keys to sort it.
To sort by values, you can use [operator](https://docs.python.org/3.6/library/operator.html).itemgetter(1)
Return a callable object that fetches item from its operand using the operand’s `__getitem__(` method. It could be used to sort results.

~~~python
import operator
fruits = [('apple', 3), ('banana', 2), ('pear', 5), ('orange', 1)]
getcount = operator.itemgetter(1)
dict(sorted(fruits, key=getcount))

~~~

`sorted` function has also a `reverse` optional argument.

~~~python
dict(sorted(fruits, key=getcount, reverse=True))

~~~

### Exercise 4.3

Create a function `reduce` to reduce the list of words returned by `map_words` and return a dictionary containing all words as keys and number of occurrences as values.

```python
recuce('sample.txt')
{'tempora': 2, 'non': 1, 'quisquam': 1, 'amet': 1, 'sit': 1}
```

You probably notice that this simple function is not easy to implement. Python standard library provides some features that can help.

## Container datatypes

`collection` module implements specialized container datatypes providing alternatives to Python’s general purpose built-in containers, `dict`, `list`, `set`, and `tuple`.

- `defaultdict` :	dict subclass that calls a factory function to supply missing values
- `Counter`	: dict subclass for counting hashable objects

### defaultdict

When you implement the `wordcount` function you probably had some problem to append key-value pair to your `dict`. If you try to change the value of a key that is not present 
in the dict, the key is not automatically created.

You can use a `try-except` flow but the `defaultdict` could be a solution. This container is a `dict` subclass that calls a factory function to supply missing values.
For example, using list as the default_factory, it is easy to group a sequence of key-value pairs into a dictionary of lists:

~~~python
from collections import defaultdict
s = [('yellow', 1), ('blue', 2), ('yellow', 3), ('blue', 4), ('red', 1)]
d = defaultdict(list)
for k, v in s:
    d[k].append(v)

dict(d)

~~~

### Exercise 4.4

- Modify the `reduce` function you wrote above by using a defaultdict with the most suitable factory.

### Counter

A Counter is a dict subclass for counting hashable objects. It is an unordered collection where elements are stored as dictionary keys and their counts are stored as dictionary values. Counts are allowed to be any integer value including zero or negative counts.

Elements are counted from an iterable or initialized from another mapping (or counter):

~~~python
from collections import Counter

violet = dict(r=23,g=13,b=23)
print(violet)
cnt = Counter(violet)  # or Counter(r=238, g=130, b=238)
print(cnt['c'])
print(cnt['r'])

~~~

~~~python
print(*cnt.elements())

~~~

~~~python
cnt.most_common(2)

~~~

~~~python
cnt.values()

~~~

### Exercise 4.5

Use a `Counter` object to count words occurences in the sample text file.

The Counter class is similar to bags or multisets in some Python libraries or other languages. We will see later how to use Counter-like objects in a parallel context.

## Process multiple files

- Create several files containing `lorem` text named 'sample01.txt', 'sample02.txt'...
- If you process these files you return multiple dictionaries.
- You have to loop over them to sum occurences and return the resulted dict. To iterate on specific mappings, Python standard library provides some useful features in `itertools` module.
- [itertools.chain(*mapped_values)](https://docs.python.org/3.6/library/itertools.html#itertools.chain) could be used for treating consecutive sequences as a single sequence.

~~~python
import itertools, operator
fruits = [('apple', 3), ('banana', 2), ('pear', 5), ('orange', 1)]
vegetables = [('endive', 2), ('spinach', 1), ('celery', 5), ('carrot', 4)]
getcount = operator.itemgetter(1)
dict(sorted(itertools.chain(fruits,vegetables), key=getcount))

~~~

### Exercise 4.6

- Write the program that creates files, processes and use `itertools.chain` to get the merged word count dictionary.

### Exercise 4.7

- Create the `wordcount` function in order to accept several files as arguments and
return the result dict.

```
wordcount(file1, file2, file3, ...)
```

[Hint: arbitrary argument lists](https://docs.python.org/3/tutorial/controlflow.html#arbitrary-argument-lists)

- Example of use of arbitrary argument list and arbitrary named arguments.

~~~python
def func( *args, **kwargs):
    for arg in args:
        print(arg)
        
    print(kwargs)
        
func( "3", [1,2], "bonjour", x = 4, y = "y")

~~~
