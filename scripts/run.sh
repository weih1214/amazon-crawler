threshold=0.90
dir='/root/scripts/'
rs=$dir'results/'
python $dir'product.py' > $rs'product_'$(date +\%F).txt
python $dir'general.py' $threshold 'Review' > $rs'review_'$(date +\%F).txt
python $dir'general.py' $threshold 'Question' > $rs'question_'$(date +\%F).txt
python $dir'offer.py' $threshold > $rs'offer_'$(date +\%F).txt
python $dir'comment.py' $threshold > $rs'comment_'$(date +\%F).txt
python $dir'answer.py' $threshold > $rs'answer_'$(date +\%F).txt
python $dir'ansComment.py' $threshold > $rs'anscomment_'$(date +\%F).txt
