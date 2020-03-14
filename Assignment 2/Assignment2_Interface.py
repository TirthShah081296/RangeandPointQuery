#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    # Implement RangeQuery Here.
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    #cur.execute("SELECT table_name FROM information_schema.tables")
    list_of_tables = cur.fetchall()
    result = []
    #rangepartition
    for t in list_of_tables:
        if "rangeratingspart" in str(t[0]):
            t_name = t[0]
            cur.execute('SELECT max(rating), min(rating) FROM {0}'.format(t_name))
            maxi, mini = cur.fetchone()
            if maxi < ratingMinValue or mini > ratingMaxValue:
                continue
            cur.execute('SELECT * from {0} WHERE rating>={1} and rating<={2}'.format(t_name, ratingMinValue, ratingMaxValue))
            temp = cur.fetchall()
            for i in temp:
                result.append((t_name, i[0], i[1], i[2]))
    #roundrobin part
    for t in list_of_tables:
        if "roundrobinratingspart" in str(t[0]):
            t_name = t[0]
            cur.execute('SELECT * from {0} WHERE rating>={1} and rating<={2}'.format(t_name, ratingMinValue, ratingMaxValue))
            temp = cur.fetchall()
            for i in temp:
                result.append((t_name, i[0], i[1], i[2]))

    file = open(outputPath, 'w')

    for i in result:
        b = i[0].replace('rangeratingspart','RangeRatingsPart')
        b = b.replace('roundrobinratingspart','RoundRobinRatingsPart')
        file.write(b +","+str(i[1]) +"," + str(i[2]) +"," + str(i[3])+"\n")

    file.close()
    openconnection.commit()

def PointQuery(ratingValue, openconnection, outputPath):
    #Implement PointQuery Here.
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    list_of_tables = cur.fetchall()
    result = []
    #rangepart
    for t in list_of_tables:
        if "rangeratingspart" in str(t[0]):
            t_name = t[0]
            cur.execute('SELECT max(rating), min(rating) FROM {0}'.format(t_name))
            maxi, mini = cur.fetchone()
            if maxi < ratingValue or mini > ratingValue:
                continue
            cur.execute(
                'SELECT * from {0} WHERE rating={1}'.format(t_name, ratingValue))
            temp = cur.fetchall()
            for i in temp:
                result.append((t_name, i[0], i[1], i[2]))
    # roundrobin part
    for t in list_of_tables:
        if "roundrobinratingspart" in str(t[0]):
            t_name = t[0]
            cur.execute(
                'SELECT * from {0} WHERE rating={1}'.format(t_name, ratingValue))
            temp = cur.fetchall()
            for i in temp:
                result.append((t_name, i[0], i[1], i[2]))

    file = open(outputPath, 'w')

    for i in result:
        b = i[0].replace('rangeratingspart', 'RangeRatingsPart')
        b = b.replace('roundrobinratingspart', 'RoundRobinRatingsPart')
        file.write(b + "," + str(i[1]) + "," + str(i[2]) + "," + str(i[3]) + "\n")

    file.close()
