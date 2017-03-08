import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_review = client.ReviewPage
db_comment = client.CommentPage

dic = {}
fail_id_list = []
fail_link_list = []
count = 0
temp2 = 0
total_number = db_review.content.count()
cursor1 = db_comment.content.aggregate([{'$group':{'_id':{'Review ID':'$Review ID','Comment ID':'$Comment ID'}, 'count':{'$sum':1}}}, {'$group':{'_id':'$Review ID', 'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[doc['_id']] = doc['count']

cursor2 = db_review.content.find({},{'Review ID':1, 'Total Comments':1, 'Comment Link':1})
for doc in cursor2:
	review_id = doc['Review ID']
	expected_val = doc['Total Comments'] if doc['Total Comments'] else 0
	actual_val = dic.get(review_id, 0)
	print 'Review ID: ',review_id,'\nActual Number: ', actual_val, '\nExpected Number: ', expected_val
	if actual_val > expected_val:
		print 'Succeeds: Actual Number Exceeds!'
		continue
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		count += 1
		continue
	print 'Fails!'
	fail_id_list.append(review_id)
	fail_link_list.append(doc['Comment Link'])

print 'Actual Number: ', count,'\nExpected Number: ', total_number, '\nPercentage: ', count/float(total_number)
print fail_id_list
with open('/root/scripts/fixList/comment.txt', 'a+') as f:
	for link in fail_link_list:
		f.write(link)
print len(fail_id_list)
