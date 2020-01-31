import pandas
import numpy

from matplotlib import pyplot as plt
from matplotlib import gridspec as gridspec

if __name__ == "__main__":
    df = pandas.read_csv("knn.csv", sep=";")
    fig, axs = plt.subplots(nrows=1, ncols=1)
    axs.set_title("KNN")
    colors = {
        "a": "red",
        "b": "blue"
    }
    df.plot(
        kind='scatter',
        x='x',
        y='y',
        color=[colors[label] for label in df['label']],
        ax=axs
    )
    plt.xlabel('x')
    plt.ylabel('y')
    plt.grid(True)
    plt.show()
