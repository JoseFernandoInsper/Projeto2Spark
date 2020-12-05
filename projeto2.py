#projeto2
import pyspark
import math
import matplotlib.pyplot as plt
from PIL import Image
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
import pandas as pd

sc = pyspark.SparkContext(appName="jojo")

rdd = sc.sequenceFile("part-00000")

rdd.count()

result = rdd.take(10)

def conta_palavras_doc(item):
    url, conteudo = item
    palavras = conteudo.strip().split()
    return [(palavra, 1) for palavra in set(palavras)]

def junta_contagens(nova_contagem, contagem_atual):
    return nova_contagem + contagem_atual

def conta_uma_palavra(item, palavra):
    url, conteudo = item
    palavras = conteudo.strip().split()
    #count = 0
    lista = [palavra.lower() for palavra in palavras]
    if palavra in lista:
        return [item]
    return []

N = rdd.count()

DOC_COUNT_MIN = 5
DOC_COUNT_MAX = 0.7*N

def filtra_doc_freq(item):
    contagem = item[1]
    return (contagem < DOC_COUNT_MAX) and (contagem >= DOC_COUNT_MIN)

def computa_idf(item):
    palavra,contagem = item
    idf = math.log10(N/contagem)
    return (palavra,idf)

def computa_freq(item):
    palavra,contagem = item
    quant = math.log10(1+contagem)
    return (palavra,quant)

def relevancia(item):
    palavra, quant = item
    freq, idf = quant
    relevancia = freq*idf
    return (palavra, relevancia)
    
if __name__ == "__main__":
    sc = pyspark.SparkContext(appName="jojo")
    rdd = sc.sequenceFile("part-00000")

    Rakin = rdd.filter(lambda x:"rakin" in x[1])
    Yoda = rdd.filter(lambda x:"yoda" in x[1])
    inter = Rakin.intersection(Yoda)

    rdd_Rakin = Rakin.flatMap(conta_uma_palavra).reduceByKey(junta_contagens)
    rdd_Yoda = Yoda.flatMap(conta_uma_palavra).reduceByKey(junta_contagens)
    rdd_inter = inter.flatMap(conta_uma_palavra).reduceByKey(junta_contagens)

    rdd_doc_rakin = Rakin.flatMap(conta_palavras_doc).reduceByKey(junta_contagens)
    rdd_doc_yoda = Yoda.flatMap(conta_palavras_doc).reduceByKey(junta_contagens)
    rdd_doc_inter = inter.flatMap(conta_palavras_doc).reduceByKey(junta_contagens)

    rdd_doc_rakin_freq_filtrado = rdd_doc_rakin.filter(filtra_doc_freq)
    rdd_doc_yoda_freq_filtrado = rdd_doc_yoda.filter(filtra_doc_freq)
    rdd_doc_inter_freq_filtrado = rdd_doc_inter.filter(filtra_doc_freq)

    rakin_rdd_idf = rdd_doc_rakin_freq_filtrado.map(computa_idf)
    yoda_rdd_idf = rdd_doc_yoda_freq_filtrado.map(computa_idf)
    inter_rdd_idf = rdd_doc_inter_freq_filtrado.map(computa_idf)

    rakin_rdd_freq = rdd_doc_rakin_freq_filtrado.map(computa_freq)
    yoda_rdd_freq = rdd_doc_yoda_freq_filtrado.map(computa_freq)
    inter_rdd_freq = rdd_doc_inter_freq_filtrado.map(computa_freq)


    rdd_rakin_join = rakin_rdd_freq.join(rakin_rdd_idf)
    rdd_yoda_join = yoda_rdd_freq.join(yoda_rdd_idf)
    rdd_inter_join = inter_rdd_freq.join(inter_rdd_idf)

    relevancia_rakin = rdd_rakin_join.map(relevancia)
    relevancia_yoda = rdd_yoda_join.map(relevancia)
    relevancia_inter = rdd_inter_join.map(relevancia)

    rakin_data = pd.DataFrame(relevancia_rakin, columns=["palavra", "relevancia"])
    yoda_data = pd.DataFrame(relevancia_yoda, columns=["palavra", "relevancia"])
    inter_data = pd.DataFrame(relevancia_inter, columns=["palavra", "relevancia"])

    rakinBD = rakin_data.to_csv("s3linknosso", index=False)
    yodaBD = yoda_data.to_csv("s3linknosso", index=False)
    interBD = inter_data.to_csvI("s3linknosso", index=False)

