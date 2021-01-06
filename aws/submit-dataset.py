def lambda_handler(event, context):
    print("Event", event)
    print("Context", context.invoked_function_arn)
    return {
        'source_id': '123-44-55-66'
    }

