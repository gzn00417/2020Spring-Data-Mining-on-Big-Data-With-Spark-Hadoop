# coding=UTF-8
import random

N = 1000
M = 100
filename = ".\data2.txt"
with open(filename, "w", encoding="utf-8") as file:
    for i in range(N):
        file.write(
            "(" + str(i + 1) + ", " + str(round((random.random() - 0.5) * M, 3)) + ")\n"
        )
