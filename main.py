from script import descript_stat
from script import plot_histogram
import pandas as pd

GY_data = pd.read_csv('Global YouTube Statistics.csv', encoding="ISO-8859-1")



#Top creators' subscriber counts, video views, upload frequency

df = GY_data[['subscribers','video views','uploads']]


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    descript_stat(df)
    plot_histogram(df)
