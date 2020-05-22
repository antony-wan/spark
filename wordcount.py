import sys
from pyspark import SparkContext

sc = SparkContext();

lines = sc.textFile(sys.argv[1])
#sys.argv[1] = parametre passe en ligne de cmd
#lines est de type RDD

all_words = lines.flatMap(lambda line: line.split(' '))
#Comme la fonction map sauf qu'il est possible d'associer plusieurs sorties a une entree
words_with_1 = all_words.map(lambda word: (word, 1))
word_counts = words_with_1.reduceByKey(lambda count1, count2: count1 + count2)
result = word_counts.collect()
#result est une liste de tuples (cle, valeur)
#La methode collect() est une action

for (word, count) in result:
    print(word, count)

#print the number of distinct words
#different_words = all_words.distinct()
#different_words_count = different_words.count()
#print('Number of words : {}'.format(different_words_count))

#first_10_words = different_words.takeOrdered(10)
#print(first_10_words)
