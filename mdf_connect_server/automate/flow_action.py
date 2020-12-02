import ast


class FlowAction:
    def __init__(self, flow, action_id: str):
        self.action_id = action_id
        self.flow = flow

    def get_status(self):
        return self.flow.get_status(self.action_id)

    def get_error_msgs(self):
        logs = self.flow.get_flow_logs(self.action_id)
        error_msgs = []
        for failure in filter(lambda x: x['code'] == 'ActionFailed', logs['entries']):
            # Failures from Search Ingest Action Provider are bundled up as string
            # representation of Python dict
            cause = ast.literal_eval(failure['details']['cause'])
            if 'errors' in cause:
                error_msgs.append(cause['errors'])

        return error_msgs
