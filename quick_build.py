import psycopg2 as db
import pickle
import operator

from MemberProduct import MemberProduct

# the idea is to build a matrix based on frequency of products
# bought by a user and also to keep in my the number of items
# a user usually puts in his basket

# see if i can randomize the picking using frequency (ab testing)

config = {}
admin_orders_table = 'admin_orders'
admin_orders_item_table = 'admin_orderitems'
admin_category_products = 'admin_categoryproducts'
member_product = {}

def read_configuration():
	execfile("configuration.conf",config)

def db_connect():
	host = 'localhost'
	port = config['postgres_port']
	dbname = config['postgres_dbname']
	user = config['postgres_user']
	password = config['postgres_password']

	connectionString = 'host = %s port=%s dbname=%s user=%s password=%s' %(host,port,dbname,user,password)
	connection = db.connect(connectionString)
	return connection

def prepare_user_data():
	conn = db_connect()
	cursor = conn.cursor()
	memberIdQuery = 'select distinct(member_id) from %s where member_id is not null'  %(admin_orders_table,)
	cursor.execute(memberIdQuery)
	memberRows = cursor.fetchall()

	# for every member get the orders
	for memberRow in memberRows:

		memberProductObj = MemberProduct()
		memberId = memberRow[0]
		print 'processing data for user %s' %(memberId,)
		#now that we have the member id, we need to get all the orders for the given memeber
		orderIdQuery = 'select id from %s where member_id = %s' %(admin_orders_table,memberId)
		cursor.execute(orderIdQuery)
		orderIdRows = cursor.fetchall()

		# for every order get the products
		for orderIdRow in orderIdRows:
			orderId = orderIdRow[0]
			productsQuery = 'select id from %s where order_id = %s' %(admin_orders_item_table,orderId)
			cursor.execute(productsQuery)
			productRows = cursor.fetchall()

			# add the product or increase its frequency for a given member
			for productRow in productRows:
				productId = productRow[0]
				memberProductObj.add_or_modify_product_freq(productId)

			
		# computing average number of products in the cart per user
		computeAverageQuery = ''' select avg(count),member_id from (
        								select count(*),member_id,order_id 
        								from 
                							(select t1.member_id,t2.order_id,t2.id 
                							from
                								(select id as "order_id",member_id  from %s where member_id = %s) as t1
                								INNER JOIN
                								(select order_id,id from %s) as t2
                								ON
                								t1.order_id = t2.order_id)as op1 
                						group by member_id,order_id)as op2 
									group by member_id ''' %(admin_orders_table,memberId,admin_orders_item_table)

		cursor.execute(computeAverageQuery)
		averageRows = cursor.fetchall()
		for averageRow in averageRows:
			average = averageRows[0]
			memberProductObj.modifyAverageItemsInBasket(average)


		# adding the memberProductObj to the output
		member_product[memberId] = memberProductObj
		print memberProductObj.getFrequencyOfProduct()
		print 

	#end of the loop

	memberKeys = member_product.keys()
	for key in memberKeys:
		memberObj = member_product[key]
		print memberObj.getFrequencyOfProduct()
		print memberObj.getAverageBasketSize()


read_configuration()
prepare_user_data()










