from polls.models import Question, Choice
    
q = Question.objects.create(question_text='is this a test?')
q2 = Question.objects.get(question_text='is this a test?')
assert(q.id == q2.id)

choice = q2.choice_set.create(choice_text="test choice")

assert(choice.question.id == q2.id)

assert (Choice.objects.get(choice_text="test choice").id == choice.id)
q.delete()

