# coding=UTF-8
import random

N = 10000
M = 100
filename = ".\data1.txt"
with open(filename, "w", encoding="utf-8") as file:
    for i in range(N):
        if i != 0:
            file.write(",")
        file.write(str(round((random.random() - 0.5) * M, 6)))
