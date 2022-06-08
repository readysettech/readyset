import aws_cdk as core
import aws_cdk.assertions as assertions

from readyset_cdk.readyset_cdk_stack import ReadysetCdkStack

# example tests. To run these tests, uncomment this file along with the example
# resource in readyset_cdk/readyset_cdk_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = ReadysetCdkStack(app, "readyset-cdk")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
