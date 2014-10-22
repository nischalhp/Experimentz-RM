import psycopg2 as db
import sys


def db_connect():

	connection = db.connect('host=localhost port=27030 dbname=ods user=ods_prod password=ods#R3dMart')
	return connection


def find_last_inserted_time():

	list_of_tables = ['comment_text',
					  'customer',
					  'cycle_count_master',
						'cycle_count_plan',
						'cycle_count_request',
						'dock_mgmt_work_data',
						'fresh_items',
						'interface_error',
						'inventory_summary',
						'item',
						'item_cross_reference',
						'launch_statistics',
						'launch_summary',
						'location',
						'location_inventory',
						'lot',
						'lot_attribute',
						'process_history',
						'qc_assignment',
						'quality_history',
						'receipt_container',
						'receipt_detail',
						'rfid_history',
						'scale_user_profile',
						'serial_number',
						'shipment_alloc_request',
						'shipment_detail',
						'shipment_header',
						'shipping_cont_qc_count',
						'shipping_cont_qc_evaluation',
						'shipping_cont_qc_reason_code',
						'shipping_container',
						'shipping_load',
						'trans_hist_attributes',
						'transaction_history',
						'user_activity',
						'warehouse_alert_request',
						'work_instruction']
	conn = db_connect()
	cursor = conn.cursor()

	for table in list_of_tables:
		query = 'SELECT inserted_date_time from %s order by inserted_date_time desc limit 1' %(table)
		try:
			cursor.execute(query)
			result = cursor.fetchall()
			row = result[0][0]
			print '\'%s\' - %s' %(table,row) 
		except IndexError as e:
			print 'Table %s does not have any entries' %(table,) 

find_last_inserted_time()





