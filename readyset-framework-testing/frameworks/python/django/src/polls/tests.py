from django.test import TestCase
from .models import Question, Choice
# Create your tests here.

class QuestionModelTests(TestCase):
    def setUp(self):
        Question.objects.create(question_text="is this a test?")

    def tearDown(self):
        Question.objects.get(question_text="is this a test?").delete()

    def test_get_question(TestCase):
        q = Question.objects.get(question_text="is this a test?")        
        