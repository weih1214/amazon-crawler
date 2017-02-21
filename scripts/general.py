import sys, pymongo
from pymongo import MongoClient

map = {'Review':'Total Reviews','Question':'Total Questions', 'Offer':'Total Offers'}
fieldName = map[sys.argv[2]]
db_name = sys.argv[2] + 'Page'
client = MongoClient('localhost', 27017)
db_product = client.ProductPage
db_para = client[db_name]

dic = dict()
groupedResult = db_para.content.aggregate([{'$group':{'_id':'$Product ID','count':{'$sum':1}}}])['result']
for doc in groupedResult:
	dic[doc['_id']] = doc['count']

fail_product_list = []
expected_number = actual_number = success_count = 0
product_id = ''
docs = db_product.content.find({},{'Product ID':1, fieldName:1})
total_count = db_product.content.count()
for doc in docs:
	product_id = doc['Product ID']
	expected_number = doc[fieldName]
	if expected_number == None:
		expected_number = 0
	actual_number = dic.get(product_id, 0)
	print "Product ID: "+product_id+"\nActual Number: "+str(actual_number)+'\nExpected Number: '+str(expected_number)
	if expected_number==0 or actual_number/float(expected_number) >= float(sys.argv[1]):
		print sys.argv[2] + ' succeeds!'
		success_count += 1
	else:
		fail_product_list.append(product_id)
print 'Actual Number of Successful Products: ' + str(success_count) + '\nExpected Number of Products: '+ str(total_count)+'\nPercentage: '+ str(success_count/float(total_count))
print fail_product_list
