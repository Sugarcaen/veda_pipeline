import aws_cdk as core
import aws_cdk.assertions as assertions

from veda.veda_stack import VedaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in veda/veda_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = VedaStack(app, "veda")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
