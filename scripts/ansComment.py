import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_Ans = client.AnswerPage
db_AnsComment = client.AnswerCommentPage

dic = {}
failure_list = []
count = 0
total_number = db_Ans.content.count()
cursor1 = db_AnsComment.content.aggregate([{'$group':{'_id':'$Answer ID', 'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[doc['_id']] = doc['count']

cursor2 = db_Ans.content.find({},{'Answer ID':1, 'Total Answer Comments':1})
for doc in cursor2:
	answer_id = doc['Answer ID']
	expected_val = doc['Total Answer Comments'] if doc['Total Answer Comments'] else 0
	actual_val = dic.get(answer_id, 0)
	print 'Review ID: ',answer_id,'\nActual Number: ', actual_val, '\nExpected Number: ', expected_val
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		count += 1
		continue
	print 'Fails!'
	failure_list.append(answer_id)

print 'Actual Number: ', count,'\nExpected Number: ', total_number, '\nPercentage: ', count/float(total_number)
print failure_list
