import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_Ans = client.AnswerPage
db_AnsComment = client.AnswerCommentPage

dic = {}
fail_id_list = []
fail_link_list = []
zero_count = 0
total_number = len(db_Ans.content.distinct('Answer ID'))
cursor1 = db_AnsComment.content.aggregate([{'$group':{'_id':{'Answer ID':'$Answer ID', 'Answer Comment ID':'$Answer Comment ID'}}},{'$group':{'_id':'$_id.Answer ID', 'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[doc['_id']] = doc['count']

id_set = set()
cursor2 = db_Ans.content.find({}, {'Answer ID':1, 'Total Answer Comments':1, 'Answer Comment Link':1})
for doc in cursor2:
	answer_id = doc['Answer ID']
	if answer_id in id_set:
		continue
	id_set.add(answer_id)
	expected_val = doc['Total Answer Comments'] if doc['Total Answer Comments'] else 0
	if expected_val == 0:
		zero_count += 1
	actual_val = dic.get(answer_id, 0)
	print 'Answer ID: ',answer_id,'\nActual Number: ', actual_val, '\nExpected Number: ', expected_val
	if actual_val > expected_val:
		print 'Succeeds: Actual Number Exceeds!'
		continue
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		continue
	print 'Fails!'
	fail_id_list.append(answer_id)
	fail_link_list.append(doc['Answer Comment Link'])

actual_number = total_number - len(fail_id_list)
print 'Actual Number: ', actual_number,'\nExpected Number: ', total_number, '\nPercentage: ', actual_number/float(total_number)
print 'Excluding zero:'
print 'Actutal Number: ', actual_number-zero_count, '\nExpected Number: ', total_number-zero_count, '\nPercentage: ', (actual_number-zero_count)/float(total_number-zero_count)
print fail_id_list
print len(fail_id_list)
