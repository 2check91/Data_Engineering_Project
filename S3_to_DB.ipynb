{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": false
   },
   "outputs": [],
   "source": [
    "from pyspark import SparkContext\n",
    "import os\n",
    "# make sure pyspark tells workers to use python3 not 2 if both are installed\n",
    "os.environ['PYSPARK_PYTHON']='/usr/bin/python3'\n",
    "\n",
    "sc = SparkContext()\n",
    "rdd = sc.read.json(\"s3a://thelastbucket/*\").cache()\n",
    "\n",
    "import json\n",
    "import psycopg2\n",
    "from psycopg2 import IntegrityError, InternalError\n",
    "\n",
    "def get_jobs(jobs):\n",
    "    try:\n",
    "        employer = jobs['employer']\n",
    "        description = jobs['description']\n",
    "        location = jobs['location']\n",
    "        title = jobs['title']\n",
    "        salary = jobs['salary']\n",
    "        rating = jobs['rating']\n",
    "        recommend = jobs['recommend']\n",
    "        return [employer,description,location,title,salary,rating,recommend]\n",
    "    except ValueError:\n",
    "        pass\n",
    "\n",
    "\n",
    "def get_employer(jobs):\n",
    "    try:\n",
    "        employer = jobs['employer']\n",
    "        rating = jobs['rating']\n",
    "        recommend = jobs['recommend']\n",
    "        return [employers,rating,recommend]\n",
    "    except ValueError:\n",
    "        pass\n",
    "\n",
    "def get_location(jobs):\n",
    "    try:\n",
    "        location = jobs['location']\n",
    "        country = 'USA'\n",
    "        return [location,country] \n",
    "    except ValueError:\n",
    "        pass\n",
    "\n",
    "    \n",
    "def beam(source):\n",
    "    conn = psycopg2.connect(**{'dbname': 'jobdata', 'host': 'glassdoor.cvkrapxu8j2q.us-east-1.rds.amazonaws.com', 'password': 'P4ssw*rd', 'user': 'ptluczek'})\n",
    "    cur = conn.cursor()\n",
    "    import os\n",
    "    os.environ['PYSPARK_PYTHON']='/usr/bin/python3'\n",
    "\n",
    "    for jobs_str in source:\n",
    "        import os\n",
    "        os.environ['PYSPARK_PYTHON']='/usr/bin/python3'\n",
    "        jobs = json.loads(jobs_str)\n",
    "        jb, em, lo = get_jobs(jobs), get_employer(jobs), get_location(jobs)\n",
    "\n",
    "        try:\n",
    "            cur.execute(\"INSERT INTO jobs VALUES (%s,%s,%s,%s,%s,%s,%s)\",jb)\n",
    "            cur.execute(\"INSERT INTO employer VALUES (%s,%s,%s)\",em)\n",
    "            cur.execute(\"INSERT INTO location VALUES (%s,%s)\",lo)                \n",
    "            conn.commit()\n",
    "        except (IntegrityError, InternalError) as e:  # prevents duplicates\n",
    "            cur.execute(\"rollback\")\n",
    "\n",
    "    conn.commit()\n",
    "    conn.close()\n",
    "\n",
    "rdd.foreachPartition(beam)"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "collapsed": true
   },
   "outputs": [],
   "source": []
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.6.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 2
}
