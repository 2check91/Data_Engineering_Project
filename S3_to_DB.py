from pyspark import SparkContext
import os
# make sure pyspark tells workers to use python3 not 2 if both are installed
os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

sc = SparkContext()
rdd = sc.read.json("s3a://thelastbucket/*").cache()

import json
import psycopg2
from psycopg2 import IntegrityError, InternalError

def get_jobs(jobs):
    try:
        employer = jobs['employer']
        description = jobs['description']
        location = jobs['location']
        title = jobs['title']
        salary = jobs['salary']
        rating = jobs['rating']
        recommend = jobs['recommend']
        return [employer,description,location,title,salary,rating,recommend]
    except ValueError:
        pass


def get_employer(jobs):
    try:
        employer = jobs['employer']
        rating = jobs['rating']
        recommend = jobs['recommend']
        return [employers,rating,recommend]
    except ValueError:
        pass

def get_location(jobs):
    try:
        location = jobs['location']
        country = 'USA'
        return [location,country] 
    except ValueError:
        pass

    
def beam(source):
    conn = psycopg2.connect(**{'dbname': 'jobdata', 'host': 'glassdoor.cvkrapxu8j2q.us-east-1.rds.amazonaws.com', 'password': 'P4ssw*rd', 'user': 'ptluczek'})
    cur = conn.cursor()
    import os
    os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

    for jobs_str in source:
        import os
        os.environ['PYSPARK_PYTHON']='/usr/bin/python3'
        jobs = json.loads(jobs_str)
        jb, em, lo = get_jobs(jobs), get_employer(jobs), get_location(jobs)

        try:
            cur.execute("INSERT INTO jobs VALUES (%s,%s,%s,%s,%s,%s,%s)",jb)
            cur.execute("INSERT INTO employer VALUES (%s,%s,%s)",em)
            cur.execute("INSERT INTO location VALUES (%s,%s)",lo)                
            conn.commit()
        except (IntegrityError, InternalError) as e:  # prevents duplicates
            cur.execute("rollback")

    conn.commit()
    conn.close()

rdd.foreachPartition(beam)
