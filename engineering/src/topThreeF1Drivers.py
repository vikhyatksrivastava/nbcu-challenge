import pandas as pd

def readSourceFile(filename):
    sourcefile = pd.read_csv(filename, encoding='latin1')
    sourcefile['TIME'] = pd.to_datetime(sourcefile['TIME'], format='%M:%S.%f').dt.time
    sourcefile['TIME'] = sourcefile['TIME'].apply(lambda time: sum(y * float(t) for y, t in zip([60, 1, 1/100], str(time).split(':'))))

    # Calculate the average time per lap
    average_time_per_lap = sourcefile.groupby('DRIVER')['TIME'].mean().sort_values(ascending=False)
    topthreedrivers = average_time_per_lap.head(3)
    print("Average time per lap of top three drivers are:", topthreedrivers)

def main():
    fileName = "../sourceFile/F1_driver_lap_status_bahrain_2021.csv"
    readSourceFile(fileName)

if __name__ == '__main__':
    main()