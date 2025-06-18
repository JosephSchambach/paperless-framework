from ff_framework.context.context_logging import ContextLogger
from ff_framework.api.api_obj_config import api_obj_config
from time import sleep


class FormFlowAPI:
    def __init__(self, logger: ContextLogger, database):
        self.logger = logger
        self.database = database
        self.obj_config = api_obj_config()
        
    def create(self, args, log = True, alert = False):
        arg_type = type(args)
        create_response = []
        if log:
            self.logger.log(f"Creating object of type {arg_type}", level='info')
        if not isinstance(args, list):
            args = [args]
        for i, arg in enumerate(args):
            self.logger.log(f"Processing {i+1} {arg_type} objects", level='info')
            kwargs = self.obj_config[arg.__class__.__name__]
            result = self._execute(arg, kwargs, alert=alert)
            if result is not None:
                actor = getattr(self, result["context_method"])
                action = getattr(actor, result["execution_method"])
                action_response = action(result["table"], result["columns"], result["values"])
                create_response.append(action_response)
        if log:
            self.logger.log(f"Created {len(create_response)} objects of type {arg_type}", level='info')
        return create_response if len(create_response) > 1 else create_response[0]
    
    def process(self, args, log = True, alert = False, retries = 0, retry_interval = 0):
        arg_type = type(args)
        process_response = []
        if log:
            self.logger.log(f"Processing object of type {arg_type}", level='info')
        if not isinstance(args, list):
            args = [args]
        for i, arg in enumerate(args):
            self.logger.log(f"Processing {i+1} {arg_type} objects", level='info')
            kwargs = self.obj_config[arg.__class__.__name__]
            result = self._execute(arg, kwargs, log=log, alert=alert, retries=retries, retry_interval=retry_interval)
            if result is not None:
                actor = getattr(self, result["context_method"])
                action = getattr(actor, result["execution_method"])
                action_response = action(result["table"], result["columns"], result["values"])
                process_response.append(action_response)
        if log:
            self.logger.log(f"Processed {len(process_response)} objects of type {arg_type}", level='info')
        return process_response if len(process_response) > 1 else process_response[0]
    
    def _execute(self, args, kwargs, log = None, alert = None, retries = 0, retry_interval = 0):
        for _ in range(retries + 1):
            try:
                parent_method = kwargs.get("parent_method")
                kwargs = kwargs.get("kwargs", {})
                if log:
                    self.logger.log(f"Processing {args} with parent method {parent_method}", level='info')
                return parent_method(args, **kwargs)
            except Exception as e:
                if log: 
                    self.logger.error(f"Error processing {args}: {e}", level='error')
                if _ == retries - 1:
                    if alert:
                        self.logger.alert(f"Failed to process {args} after {retries} retries", level='error')
                    raise e
                sleep(retry_interval)
                continue
    