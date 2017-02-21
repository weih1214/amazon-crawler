import pymongo, sys
client = pymongo.MongoClient('localhost', 27017)
db_question = client.QuestionPage
db_answer = client.AnswerPage

dic = {}
count, total_number = 0, db_question.content.count()
fail_list = []
cursor1 = db_answer.content.aggregate([{'$group':{'_id':{'question':'$Question ID','product':'$Product ID'},'count':{'$sum':1}}}])['result']
for doc in cursor1:
	dic[(doc['_id']['question'], doc['_id']['product'])] = doc['count']

cursor2 = db_question.content.find({},{'Question ID':1, 'Product ID':1, 'Total Answers':1})
for doc in cursor2:
	temp = (doc['Question ID'], doc['Product ID'])
	expected_val = doc['Total Answers'] if doc['Total Answers'] else 0
	actual_val = dic.get(temp, 0)
	print 'Question ID: ',temp[0], '   Product ID: ',temp[1],'\nActual Number: ',str(actual_val),'\nExpected Number: ',str(expected_val)
	if expected_val == 0 or actual_val / float(expected_val) >= float(sys.argv[1]):
		print 'Succeeds!'
		count += 1
		continue
	print 'Fails!'
	fail_list.append(temp)
print 'Actual Number: '+str(count)+ '\nExpected Number: '+str(total_number)+'\nPercentage: '+ str(count/float(total_number))
print '\nFailure List: '+str(fail_list)
