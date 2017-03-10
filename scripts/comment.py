import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_review = client.ReviewPage
db_comment = client.CommentPage

dic = {}
fail_id_list = []
fail_link_list = []
temp2 = 0
total_number = len(db_review.content.distinct('Review ID'))
cursor1 = db_comment.content.aggregate([{'$group':{'_id':{'Review ID':'$Review ID','Comment ID':'$Comment ID'}, 'count':{'$sum':1}}}, {'$group':{'_id':'$_id.Review ID', 'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[doc['_id']] = doc['count']

zero_count = 0
id_set = set()
cursor2 = db_review.content.find({}, {'Review ID':1, 'Total Comments':1, 'Comment Link':1})
for doc in cursor2:
	review_id = doc['Review ID']
	if review_id in id_set:
		continue
	id_set.add(review_id)
	expected_val = doc['Total Comments'] if doc['Total Comments'] else 0
	if expected_val == 0:
		zero_count += 1
	actual_val = dic.get(review_id, 0)
	print 'Review ID: ',review_id,'\nActual Number: ', actual_val, '\nExpected Number: ', expected_val
	if actual_val > expected_val:
		print 'Succeeds: Actual Number Exceeds!'
		continue
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		continue
	print 'Fails!'
	fail_id_list.append(review_id)
	fail_link_list.append(doc['Comment Link'])

actual_count = total_number - len(fail_id_list)
print 'Actual Number: ', actual_count,'\nExpected Number: ', total_number, '\nPercentage: ', actual_count/float(total_number)
print 'Excluding zero:'
print 'Actual Number: ', actual_count-zero_count,'\nExpected Number: ', total_number-zero_count, '\nPercentage: ', (actual_count-zero_count)/float(total_number-zero_count)
print fail_id_list
with open('/root/scripts/fixList/comment.txt', 'a+') as f:
	for link in fail_link_list:
		f.write(link)
print len(fail_id_list)
