import psycopg2 as db
import pickle
import operator

product_to_product = {}

def db_connect():

	connection = db.connect('host=localhost port=27030 dbname=ods user=ods_prod password=ods#R3dMart')
	return connection


def build_relation():

	conn = db_connect()
	cursor = conn.cursor()
	get_orders = '''select distinct(ordernum) from e9_orderdtl'''
	cursor.execute(get_orders)
	rows = cursor.fetchall()

	for ordernum in rows:
		order = ordernum[0]
		print "processing data for order %s"  %(order) 
		query = ''' select t1.linedesc as prod1,t2.linedesc as prod2 from
					(select linedesc from e9_orderdtl where ordernum=%s) as t1
					CROSS JOIN
					(select linedesc from e9_orderdtl where ordernum=%s) as t2
					WHERE 
					t1.linedesc != t2.linedesc
					'''
		cursor.execute(query,(order,order))
		linedesc = cursor.fetchall()

		for prods_row in linedesc:
			product1 = prods_row[0]

			print "analyzing for product %s" %(product1)

			if product1 in product_to_product:
				related_products = product_to_product[product1]
				product2 = prods_row[1]

				if product2 in related_products:
					occurences = related_products[product2]
					occurences = occurences + 1
					related_products[product2] = occurences
					product_to_product[product1] = related_products
				else:
					related_products[product2] = 1
					product_to_product[product1] = related_products

			else:
				product_to_product[product1] = {}


	for product in product_to_product.keys():
		print "product relation between product 1 %s and other products are" %(product),product_to_product[product]
		break


	with open("product_to_product_dict.txt","wb") as myFile:
		pickle.dump(product_to_product,myFile)

def read_output():
	output_dict = pickle.load(open("product_to_product_dict.txt","rb"))
	for product in output_dict.keys():
		related_products = output_dict[product]
		sorted_dict = sorted(related_products.items(),key=operator.itemgetter(1),reverse=True)		
		print product
		for item in sorted_dict:
			item_list = list(item)
			if item_list[1] > 40:
				print item
		break



#build_relation()
read_output()









