import sys
import psycopg2 as db

sys.path.insert(0,'/Users/nischalhp/Downloads/softwares/analytics/spark-1.1.0-bin-hadoop2.4/python')
sys.path.insert(0,'/Users/nischalhp/Downloads/softwares/analytics/spark-1.1.0-bin-hadoop2.4/python/lib/py4j-0.8.2.1-src.zip')
execfile('/Users/nischalhp/Downloads/softwares/analytics/spark-1.1.0-bin-hadoop2.4/python/pyspark/shell.py')

from pyspark.mllib.recommendation import ALS

def get_db_connection():
    connection = db.connect('host=localhost port=27030 dbname=ods user= password=')
    return connection


def prepare_data():
    conn = get_db_connection()
    cursor = conn.cursor()
    admin_orders_table = 'admin_orders'
    admin_orders_item_table = 'admin_orderitems'
    admin_category_products = 'admin_categoryproducts'
    member_query = 'select distinct(member_id) from %s' %(admin_orders_table,)
    cursor.execute(member_query)
    resultset = cursor.fetchall()

    with open('member_item_file', 'w') as write_file:

        for member_row in resultset:
            member_id = member_row[0]
            # the below query gives all the products bought and not bought by a user
            query_with_bought_flag = '''
									 SELECT t4.product_id,(CASE when t3.product_id is null then 0 else 1 end) as
									 bought_flag
    								FROM
					                (
					                select t2.id as product_id 
					                FROM
					                        (select id from admin_orders where member_id = %s )as t1
					                        INNER JOIN
					                        (select order_id,id from %s) as t2
					                        ON
					                        t1.id = t2.order_id
					                ) as t3
					                RIGHT OUTER JOIN
					                (
					                select product_id from %s
					                )as t4
					                ON
					                t4.product_id = t3.product_id
					                ''' %(member_id,admin_orders_item_table,admin_category_products)

            cursor.execute(query_with_bought_flag)
            resultset_products = cursor.fetchall()

            for products in resultset_products:
                #print member_id,products[0],products[1]
                write_file.write('%s,%s,%s \n' %(member_id,products[0],products[1]))

def train_and_predict():

    training = sc.textFile('member_item_file').map(parseRating).cache()
    #now train the model using ALS
    rank=10
    number_of_iterations = 10
    model = ALS.train(training, rank, number_of_iterations)
    print model


def parseRating(line):

    fields = line.split(',')
    # user,product,rating
    return (fields[0],fields[1],fields[2])

#def main():

    #prepare_data()
    #train_and_predict()

#main()





