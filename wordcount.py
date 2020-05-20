import sys
from pyspark import SparkContext

sc = SparkContext();
#Instancie un objet de type SparkContext qui gère les propriétés globales de l'application

lines = sc.textFile(sys.argv[1])
#sys.argv[1] = paramètre passé en ligne de cmd
#lines est de type RDD

word = lines.flatMap(lambda line: line.split(' '))
#Comme la fonction map sauf qu'il est possible d'associer plusieurs sorties à une entree
words_with_1 = word.map(lambda word: (word, 1))
word_counts = words_with_1.reduceByKey(lambda count1, count2: count1 + count2)
result = word_counts.collect()
#result est une liste de tuples (clé, valeur)
#La methode collect() est une action

for (word, count) in word_counts:
    print(word, count)
