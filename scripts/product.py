import pymongo
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db_masterlist = client.Masterlist
db_product = client.ProductPage

expected_number_on_page = db_masterlist.content.find_one()['Total Products']
expected_number = len(db_masterlist.content.distinct('Product ID'))
actual_number = len(db_product.content.distinct('Product ID'))

cursor1 = db_masterlist.content.find({'Product ID':{'$exists':True}},{'Product ID':1})
expect_set = set()
for doc in cursor1:
	expect_set.add(doc['Product ID'])

cursor2 = db_product.content.find({},{'Product ID':1})
actual_set = set()
for doc in cursor2:
	actual_set.add(doc['Product ID'])

print 'Expected Number On Page: ', expected_number_on_page, '\nExpected Number: ', expected_number, '\nActual Number: ', actual_number
print 'Missed Product List: ', list(expect_set-actual_set), '\nRedundant Product List: ', list(actual_set-expect_set)

fail_id_list = list(expect_set - actual_set)
print len(fail_id_list)
with open('/root/scripts/fixList/product.txt', 'a+') as f:
	for id in fail_id_list:
		f.write(db_masterlist.content.find_one({'Product ID':id},{'Product ID':1, 'URL':1})['URL']+'\n')
