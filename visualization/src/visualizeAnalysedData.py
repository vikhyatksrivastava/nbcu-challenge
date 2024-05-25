import pandas as pd
import matplotlib.pyplot as plt
import os

def plotPie(filename, pngPath):
    csvDf = pd.read_csv(filename)
    top_10 = csvDf.head(10)
    plt.figure(figsize=(8, 8))
    plt.pie(top_10['count'], labels=top_10['datetime'], autopct='%1.1f%%')
    plt.title('Top 10 Records by Reader Count')
    plt.savefig(pngPath)

def plotBarChart(filename, pngPath):
    csvDf = pd.read_csv(filename)
    top_10 = csvDf.head(10)
    plt.figure(figsize=(8, 8))
    plt.bar(top_10['datetime'], top_10['count'], color='skyblue')
    #plt.bar(top_10['count'], labels=top_10['datetime'], autopct='%1.1f%%')
    plt.title('Top 10 Records by Reader Count')
    plt.xlabel('datetime')
    plt.ylabel('count')
    if os.path.exists(pngPath):
        os.remove(pngPath)
    plt.savefig(pngPath)
def main():
    fileName1 = "../peacock-de-eval/output/Daily_Item_Count_Rated.csv"
    pngPath1 = "../peacock-de-eval/output/Daily_Item_Count_Rated.png"
    fileName2 = "../peacock-de-eval/output/Daily_Count_of_Rating.csv"
    pngPath2 = "../peacock-de-eval/output/Daily_Count_of_Rating.png"
    plotPie(fileName1,pngPath1)
    plotBarChart(fileName2, pngPath2)

if __name__ == '__main__':
    main()