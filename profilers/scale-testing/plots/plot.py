#!/usr/bin/env python

"""
Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.

Except as expressly permitted in a written agreement between you or your company
and Hortonworks, Inc. or an authorized affiliate or partner thereof, any use,
reproduction, modification, redistribution, sharing, lending or other exploitation
of all or any part of the contents of this software is strictly prohibited.
"""

import matplotlib.pyplot as plt

def plot_time_vs_num_columns(x = [1, 10, 50, 100, 200, 500], y = [i / 60.0 for i in [82, 81, 4710, 185, 345, 345]]):
    plt.plot(x, y)
    plt.xlabel('Number of columns')
    plt.ylabel('Execution time(in minutes)')
    plt.title('Execution time vs number of columns (1000 rows, 6 cores)')
    plt.show()

def plot_time_vs_table_size(x = [0.004, 0.08, 0.65, 1.5, 10, 16, 22, 33, 150, 300], y = [1.7, 8.6, 14, 18, 46, 22, 84, 114, 2880, 4620]):
    plt.plot(x, y)
    plt.xlabel('Table Size(GB)')
    plt.ylabel('Execution time(min)')
    plt.title('Executor memory - 10GB, Cores - 6')
    plt.show()

def plot_text_vs_orc_vs_size():
    x = [0.004, 0.08, 0.65, 1.5, 10, 16, 22, 33]
    y = [[1.1, 2.2, 4, 5.1, 9.1, 5.7, 20, 18], [1.1, 1.4, 1.7, 2.4, 40, 5.1, 35, 31], [1.7, 8.6, 14, 18, 46, 22, 84, 114]]

    plt.xlabel("Table Size(GB)")
    plt.ylabel("Execution Time(min)")
    plt.plot(x, y[2], 'b--', label= "Default", linewidth=4.0)
    plt.plot(x, y[1], 'g--', label="Optimized(ORC)", linewidth=4.0)
    plt.plot(x, y[0], 'r--', label ="Optimized(Text)", linewidth=4.0)
    plt.legend(loc='best')
    plt.show()

def plot_poor_queue_times():
    x = [0.002, 0.077, 0.106, 0.257, 1, 2.1, 3.2, 8, 14.4, 29, 38]
    y = [1, 1.5, 2.3, 3.2, 9.28, 18.25, 25, 44, 108, 270, 370]
    plt.xlabel("Table Size(GB)")
    plt.ylabel("Execution time(min)")
    plt.plot(x, y, linewidth=4.0)
    plt.title("Execution times on poor queue(5% of cluster capacity), 100 GB TPC")
    plt.show()

def plot_large_tables():
    x = [150, 300, 400]
    y = [[48, 77, 80], [1.7, 28.5, 40.6]]

    plt.xlabel("Table Size(GB)")
    plt.ylabel("Execution Time(hr)")
    plt.plot(x, y[0], 'b--', label= "Default", linewidth=4.0)
    plt.plot(x, y[1], 'g--', label="Optimized(Text)", linewidth=4.0)
    plt.title("Execution times of Large tables")
    plt.legend(loc='best')
    plt.show()

def plot_same_table_diff_dataset():
    x = [0.75, 1.8, 3.7, 7.4, 18.8, 38]
    y1 = [3.6, 3.78, 3.86, 5.18, 8.7, 14.95]
    y2 = [11.73, 13.28, 16.48, 21.71, 34.9, 36.2]
    plt.xlabel("Table Size(GB)")
    plt.ylabel("Execution time(min)")
    plt.plot(x, y1, 'b', label="Text", linewidth=4.0)
    plt.plot(x, y2, 'g', label="ORC", linewidth=4.0)
    plt.title("Execution times across TPC datasets(table - store_sales)")
    plt.legend(loc='best')
    plt.show()



if __name__ == "__main__":
    #plot_time_vs_num_columns()
    #plot_time_vs_table_size()
    #plot_text_vs_orc_vs_size()
    #plot_large_tables()
    #plot_poor_queue_times()
    plot_same_table_diff_dataset()


