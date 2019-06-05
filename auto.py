
import pyodbc
import re
import argparse

def create_table(table_name):
	return "CREATE TABLE {0}(id int not null PRIMARY KEY, bee bit, batch nvarchar(50), cluster_id nvarchar(50));".format(table_name)

def delete_table(table_name):
	return "DELETE FROM {}".format(table_name)

def fill_data(data, table_name):
    tmp = "INSERT INTO {} (id,bee,batch,cluster_id) VALUES ".format(table_name)
    for item in data:
        tmp += "({},{},'{}','{}'),".format(item[0], item[1], item[2], item[3])
    return tmp[:-1] + ";"

def count_cluster(table_name, cluster_name):
	return "select COUNT (*) from {} where cluster_id = '{}'".format(table_name, cluster_name)

def count_cluster_bee(table_name, cluster_name, bee='0'):
	return "select count(*) from {} where cluster_id = '{}' AND bee = '{}'".format(table_name, cluster_name, bee)

def bee_percent(count_cluster, count_cluster_bee):
	return count_cluster / count_cluster_bee

def readfile (file):
    
    is_line_ignored = True
    output = list()
    unique_clusters = set()

    for line in open(file):
        # zacne parsovat az kdyz najde @data
        if line.strip() == '@data':
            is_line_ignored = False
            continue

        if is_line_ignored:
            continue


        # tu bude parsovanie
        line = re.sub(r"'([0-9]+),([0-9]+)'", '$1.$2', line )
        columns = line.split(',')
        id = columns[0].strip()
        bee = columns[13].strip()
    
        if bee == 'true':
            bee = 1
        else:
            bee = 0

        batch = columns[14].strip()
        cluster_id = columns[15].strip()

        unique_clusters.add(cluster_id)

        record = [ id, bee, batch, cluster_id ]
        output.append(record)

    return [len(unique_clusters), output]
	
if __name__ == '__main__':
	#a = [6, [['0', 0, 'missing-queen-2', 'cluster5'], ['1', 0, 'missing-queen-2', 'cluster1'], ['2', 1, 'active-day-2', 'cluster1'], ['3', 1, 'missing-queen-2', 'cluster0'], ['4', 1, 'missing-queen-2', 'cluster5'], ['5', 1, 'active-day-2', 'cluster5'], ['6', 1, 'active-day-2', 'cluster4'], ['7', 1, 'active-day-2', 'cluster1'], ['8', 1, 'active-day-2', 'cluster4'], ['9', 0, 'missing-queen-2', 'cluster5'], ['10', 0, 'active-day-2', 'cluster2'], ['11', 1, 'active-day', 'cluster5'], ['12', 1, 'missing-queen', 'cluster3'], ['13', 1, 'active-day', 'cluster4'], ['14', 1, 'active-day', 'cluster2'], ['15', 0, 'active-day-2', 'cluster1'], ['16', 1, 'missing-queen-2', 'cluster5'], ['17', 1, 'missing-queen-2', 'cluster1'], ['18', 1, 'active-day-2', 'cluster4']]]

	parser = argparse.ArgumentParser()
	parser.add_argument('--file', help='weka file')
	args = parser.parse_args()

	data= readfile(args.file)

	cursor=pyodbc.connect('Trusted_Connection=yes',
	               driver='{SQL Server}', server='localhost\SQLEXPRESS',
	               database='vcely', MARS_Connection='Yes')
	"""rows = cursor.execute("SELECT COUNT (*) FROM vcely.dbo.em WHERE cluster_id = 'cluster0';"). fetchall()

	for row in rows:
		print(row)
	"""
	#print(fill_data(a,"vcely.dbo.test1"))
	#cursor.execute(create_table("vcely.dbo.test1"))
	cursor.execute(delete_table("vcely.dbo.test1"))

	tmpdata = [data[1][x:x+999] for x in range(0, len(data[1]), 999)]
	for item in tmpdata:
	#cursor.execute(fill_data(data,"vcely.dbo.test1"))
		cursor.execute(fill_data(item,"vcely.dbo.test1"))
	
	#print(count_cb)
	
	cluster_no = data[0]
	for x in range(0, cluster_no):
		cluster = 'cluster{}'.format(x)
		count_c = cursor.execute(count_cluster("vcely.dbo.test1", cluster)).fetchall()
		count_cb = cursor.execute(count_cluster_bee("vcely.dbo.test1", cluster)).fetchall()
#print(count_c)
		percentage = bee_percent(count_cb[0][0], count_c[0][0])
		print("{}: {}".format(cluster, percentage))
	
	cursor.close()