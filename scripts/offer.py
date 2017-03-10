import sys, pymongo
from pymongo import MongoClient

fieldName = 'Total Offers'
link = 'Offer Link'
client = MongoClient('localhost', 27017)
db_product = client.ProductPage
db_para = client.OfferPage

dic = dict()
groupedResult = db_para.content.aggregate([{"$group":{"_id":{"Product ID":"$Product ID", 'Seller ID':'$Seller ID', 'Price':'$Price'}}}, {"$group":{"_id":"$_id.Product ID", "count":{"$sum":1}}}])['result']
for doc in groupedResult:
	dic[doc['_id']] = doc['count']

fail_id_list = []
fail_link_list = []
expected_number = actual_number = zero_count = 0
product_id = ''

docs = db_product.content.aggregate([{'$group':{'_id':{'Product ID':'$Product ID', 'Total Offers':'$Total Offers', 'Offer Link':'$Offer Link'}}}])['result']
total_count = len(db_product.content.distinct('Product ID'))
for doc in docs:
	product_id = doc['_id']['Product ID']
	expected_number = doc['_id']['Total Offers']
	if expected_number == None:
		expected_number = 0
		zero_count += 1
	actual_number = dic.get(product_id, 0)
	print "Product ID: "+product_id+"\nActual Number: "+str(actual_number)+'\nExpected Number: '+str(expected_number)
	if actual_number > expected_number:
		print 'Succeeds: Actual Number Exceeds!'
		continue
	if expected_number==0 or actual_number/float(expected_number) >= float(sys.argv[1]):
		print 'Offer succeeds!'
	else:
		fail_id_list.append(product_id)
		fail_link_list.append(doc['_id']['Offer Link'])

success_count = total_count - len(fail_id_list)
print 'Actual Number of Successful Products: ' + str(success_count) + '\nExpected Number of Products: '+ str(total_count)+'\nPercentage: '+ str(success_count/float(total_count))
print 'Excluding zero:'
print 'Actual Number of Successful Products: ' + str(success_count-zero_count) + '\nExpected Number of Products: '+ str(total_count-zero_count)+'\nPercentage: '+ str((success_count-zero_count)/float(total_count-zero_count))
print fail_id_list
filePath = '/root/scripts/fixList/offer.txt'
with open(filePath, 'a+') as f:
	for k in fail_link_list:
		f.write(k + '\n')
