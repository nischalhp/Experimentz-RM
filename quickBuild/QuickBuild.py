import psycopg2 as db
import pickle
import operator
import collections

from MemberProduct import MemberProduct

class QuickBuild:

    config = {}
    admin_orders_table = 'admin_orders'
    admin_orders_item_table = 'admin_orderitems'
    admin_category_products = 'admin_categoryproducts'

    def read_configuration(self):
            execfile("configuration.conf",self.config)

    def db_connect(self):
            host = 'localhost'
            port = self.config['postgres_port']
            dbname = self.config['postgres_dbname']
            user = self.config['postgres_user']
            password = self.config['postgres_password']

            connectionString = 'host = %s port=%s dbname=%s user=%s password=%s' %(host,port,dbname,user,password)
            connection = db.connect(connectionString)
            return connection

    def prepare_user_data(self):
            conn = self.db_connect()
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
                    orderIdQuery = 'select id from %s where member_id = %s' %(self.admin_orders_table,memberId)
                    cursor.execute(orderIdQuery)
                    orderIdRows = cursor.fetchall()

                    # for every order get the products
                    for orderIdRow in orderIdRows:
                            orderId = orderIdRow[0]
                            productsQuery = 'select id from %s where order_id = %s' %(self.admin_orders_item_table,orderId)
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
                                                                            group by member_id ''' %(self.admin_orders_table,memberId,self.admin_orders_item_table)

                    cursor.execute(computeAverageQuery)
                    averageRows = cursor.fetchall()
                    for averageRow in averageRows:
                            average = averageRows[0]
                            memberProductObj.modifyAverageItemsInBasket(average)


                    # adding the memberProductObj to the output
                    member_product[memberId] = memberProductObj
                    #print memberProductObj.getFrequencyOfProduct()

            #end of the loop

            #memberKeys = member_product.keys()
            #for key in memberKeys:
            #        memberObj = member_product[key]
            #        print memberObj.getFrequencyOfProduct()
            #        print memberObj.getAverageBasketSize()



    def prepare_user_data_based_on_freq(self):
            member_product = {}
            conn = self.db_connect()
            cursor = conn.cursor()
            memberIdQuery = 'select distinct(member_id) from %s where member_id is not null'  %(self.admin_orders_table,)
            cursor.execute(memberIdQuery)
            memberRows = cursor.fetchall()

            number_of_members = 100
            counter = 0
            # for every member get the orders
            for memberRow in memberRows:

                    counter += 1
                    if counter == number_of_members:
                        break

                    memberProductObj = MemberProduct()
                    memberId = str(memberRow[0])
                    print 'processing data for user %s' %(memberId,)

                    #now that we have the member id, we need to get all the orders for the given memeber
                    # and categorize them into a week and with the frequency of the product

                    productFrequencyPerWeekQuery = '''SELECT WEEK,PRODUCT_ID,COUNT(PRODUCT_ID) AS FREQUENCY FROM
                           (SELECT PROPER_DAY,(CASE WHEN PROPER_DAY <= 7.0 THEN 1
                           WHEN PROPER_DAY > 7.0 AND PROPER_DAY <= 14.0 THEN 2
                           WHEN PROPER_DAY > 14.0 AND PROPER_DAY <= 21.0 THEN 3
                           WHEN PROPER_DAY > 21 AND PROPER_DAY <= 31.0 THEN 4
                           END) AS WEEK,
                           CREATED_AT,PRODUCT_ID FROM
                           (select EXTRACT(DAY FROM CREATED_AT) as PROPER_DAY,
                           CREATED_AT,ID as PRODUCT_ID
                           FROM
                           (select ID as ORDER_ID,CREATED_AT from %s where 
                           member_id = %s)as t1
                           INNER JOIN
                           (select ID,ORDER_ID from %s) as t2
                           on t1.order_id = t2.order_id)
                           as op1) as op2
                           GROUP BY WEEK,PRODUCT_ID ''' %(self.admin_orders_table,memberId,self.admin_orders_item_table)

                    cursor.execute(productFrequencyPerWeekQuery)
                    resultSetProductFreqWeek = cursor.fetchall()
                    
                    weekProductHash = {}

                    for productFreqRow in resultSetProductFreqWeek:
                        week = productFreqRow[0]
                        productId = str(productFreqRow[1])
                        frequency = int(productFreqRow[2])

                        weekKeys = weekProductHash.keys()
                        if week in weekKeys:
                            productFreq = weekProductHash[week]
                            productFreq[productId] = frequency
                            weekProductHash[week] = productFreq
                        else :
                            productFreq = {}
                            productFreq[productId] = frequency
                            weekProductHash[week] = productFreq
                        
                     

                    # this completes iterating over the resultset for a given user
                    memberProductObj.setFreqMap(weekProductHash)
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
                                                                            group by member_id ''' %(self.admin_orders_table,memberId,self.admin_orders_item_table)
                    

                    cursor.execute(computeAverageQuery)
                    averageRows = cursor.fetchall()
                    for averageRow in averageRows:
                            average = int(averageRow[0])
                            memberProductObj.modifyAverageItemsInBasket(average)
                    

                    # adding the memberProductObj to the output
                    member_product[memberId] = memberProductObj
                    #print memberProductObj.getFrequencyOfProduct()

            #end of the loop

    #	memberKeys = member_product.keys()
    #	for key in memberKeys:
    #		memberObj = member_product[key]
    #		print memberObj.getFrequencyOfProduct()
    #		print memberObj.getAverageBasketSize()
    #                break
            
            return member_product


    def user_based_freq_recommendation(self):
        self.read_configuration()
        userData = self.prepare_user_data_based_on_freq()
        return userData


    # calling the methods
    #read_configuration()
    #prepare_user_data_based_on_freq()
    #prepare_user_data()










