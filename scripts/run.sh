threshold=0.90
dir='/root/scripts/'
rsDir=$dir'result/'
python $dir'product.py' > $rsDir'product_'$(date +\%F).txt
python $dir'general.py' $threshold 'Review' > $rsDir'review_'$(date +\%F).txt
python $dir'general.py' $threshold 'Question' > $rsDir'question_'$(date +\%F).txt
python $dir'general.py' $threshold 'Offer' > $rsDir'offer_'$(date +\%F).txt
python $dir'comment.py' $threshold > $rsDir'comment_'$(date +\%F).txt
python $dir'answer.py' $threshold > $rsDir'answer_'$(date +\%F).txt
python $dir'ansComment.py' $threshold > $rsDir'anscomment_'$(date +\%F).txt
