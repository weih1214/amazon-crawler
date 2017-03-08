import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_question = client.QuestionPage
db_answer = client.AnswerPage

dic = {}
total_number = len(db_question.content.distinct('Question ID'))
fail_id_list = []
fail_link_list = []

cursor1 = db_answer.content.aggregate([{'$group':{'_id':{'Question ID':'$Question ID', 'Answer ID':'$Answer ID'}, 'count':{'$sum':1}}}, {'$group':{'_id':'$_id.Question ID', 'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[doc['_id']] = doc['count']

cursor2 = db_question.content.aggregate([{'$group':{'_id':{'Question ID':'$Question ID', 'Total Answers':'$Total Answers', 'Answer Link':'$Answer Link'}}}])['result']

for doc in cursor2:
	question_id = doc['_id']['Question ID']
	expected_val = doc['_id']['Total Answers'] if doc['_id']['Total Answers'] else 0
	actual_val = dic.get(question_id, 0)
	print 'Question ID: ', question_id, '\nActual Number: ',str(actual_val),'\nExpected Number: ',str(expected_val)
	if actual_val > expected_val:
		print 'Succeeds: Actual Number Exceeds!'
		continue
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		continue
	print 'Fails!'
	fail_id_list.append(question_id)
	fail_link_list.append(doc['_id']['Answer Link'])

actual_number = total_number - len(fail_id_list)
print 'Actual Number: '+str(actual_number)+ '\nExpected Number: '+str(total_number)+'\nPercentage: '+ str(actual_number/float(total_number))
print '\nFailure List: '+str(fail_id_list)
with open('/root/scripts/fixList/answer.txt', 'a+') as f:
	for link in fail_link_list:
		f.write(link + '\n')
print len(fail_id_list)
