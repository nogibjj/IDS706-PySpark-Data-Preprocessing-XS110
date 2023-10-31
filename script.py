import pandas as pd
import matplotlib.pyplot as plt

def descript_stat(df):

    # Head 5 rows
    print("=== Dataset Overview ===")
    print(df.head())
    print("\n")

    # Summary statistics using the describe method
    summary_stats = df.describe()
    print("=== Descriptive Statistics Overview ===")
    print(summary_stats)
    print("\n")

    # Mean, Median and Mode
    print("=== Mean, Median and Mode Overview ===")
    mean = df.mean()
    median = df.median()
    mode = df.mode().iloc[0]  # In case there are multiple modes
    print("Mean:\n", mean)
    print("Median:\n", median)
    print("Mode:\n", mode)
    print("\n")

    # Variance and standard deviation
    variance = df.var()
    std_deviation = df.std()
    print("=== Variance and standard deviation Overview ===")
    print("Variance:\n", variance)
    print("Standard Deviation:\n", std_deviation)


    # #visualization
        
    # # Create a histogram
    # plt.hist(df[['subscribers']], bins=20, color='blue', alpha=0.7)

    # # Add labels and a title
    # plt.xlabel('Value')
    # plt.ylabel('Frequency')
    # plt.title('Histogram of subscribers')

    # # Display the histogram
    # plt.show()

def plot_histogram(df):
    """
    Plot a histogram 

    """
    plt.hist(df[['subscribers']], bins=20, color='blue', alpha=0.7)
    plt.xlabel('Value')
    plt.ylabel('Frequency')
    plt.title('Histogram of subscribers')
    plt.show()

GY_data = pd.read_csv('Global YouTube Statistics.csv', encoding="ISO-8859-1")



#Top creators' subscriber counts, video views, upload frequency

data = GY_data[['subscribers','video views','uploads']]

if __name__ == "__main__":
    descript_stat(data)
    plot_histogram(data)