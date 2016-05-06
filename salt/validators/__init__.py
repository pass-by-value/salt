from salt.command.decorator import CommandDecorator


class BaseValidator(CommandDecorator):
    '''
    Base class for validators
    '''
    def validate(self):
        raise NotImplementedError('Base validator not implemented')

class FunctionValidator(BaseValidator):
    '''
    Checks if the function is a text
    '''

# class DecoratedCommand(Command):
#     '''
#     The Command Decorator class
#     '''
#     def __init__(self, prev_cmd=None):
#         '''
#         Initialize decorator info
#         '''
#         super(DecoratedCommand, self).__init__()
#         print('Called decorated command')
#         self.messages = []
#         self.prev_cmd = prev_cmd
#
#     def execute(self):
#         '''
#         :return: Return the result of the previous command
#         '''
#         if self.prev_cmd:
#             return self.prev_cmd.execute()
#
#     def __repr__(self):
#         '''
#         :return:
#         '''
#
#         return super(DecoratedCommand, self).__repr__()
#
#
# class ConcreteCommand(Command):
#     def execute(self):
#         self.messages.append('In the Concrete Command class whee!!!')
#         print ''.join(self.messages)
#
#
# class ValidationError(Command):
#     '''
#     The error class
#     '''
#     def __init__(self, err_msg):
#         self.err_msg = err_msg
#
#     def __repr__(self):
#         if self.err_msg:
#             return 'Salt command validation error {0}'.format(self.err_msg)
#         return 'Salt command validation error without information'
#
#
# class ValidatedCommand(DecoratedCommand):
#     '''
#     The class to be used by clients
#     '''
#     def execute(self):
#         if self._validate():
#             self.prev_cmd.execute()
#         else:
#             raise ValidationError(self.error)
#
#     def _validate(self):
#         self.error = ''
#         return True
#
#
# class MinionCommand(DecoratedCommand):
#     '''
#     Adds the client
#     '''
#
#     def __init__(self, **kwargs):
#         '''
#         :param prev_cmd: Previous command to execute
#         '''
#         super(DecoratedCommand, self).__init__(**kwargs)
#         self.client = self.LOCAL
#         self.messages.append(' the client is {0} '.format(self.client))
#
#     def __repr__(self):
#         return super(DecoratedCommand, self).__repr__()
#
#
# class SyncCommand(MinionCommand):
#     '''
#     Instantiate for sync behavior
#     '''
#     def __init__(self, fun=None, **kwargs):
#         super(SyncCommand, self).__init__(**kwargs)
#         if fun is None:
#             raise ValidationError('Please specify the function to use')
#         self.messages.append(
#             ' the function to execute is {}'.format(self.fun))
#         self.fun = fun
#
#     def __repr__(self):
#         return super(SyncCommand, self).__repr__()
#
#
# class AsyncCommand(MinionCommand):
#     '''
#     Instantiate for sync behavior
#     '''
#     def __init__(self, fun=None, **kwargs):
#         super(MinionCommand, self).__init__(**kwargs)
#         if fun is None:
#             raise ValidationError('Please specify the function to use')
#         self.messages.append(
#             ' the function to execute is {}'.format(self.fun))
#         self.client += '_async'
#         self.fun = fun
#
#     def __repr__(self):
#         return super(AsyncCommand, self).__repr__()

# def test_decorator_class()
# def test_command_class():
#     cmd = Command()
#     assert(cmd.tgt is None)
#     assert (cmd.tgt_type is None)
#     assert (cmd.fun is Command.FUN)
#     assert (cmd.client is Command.CLIENT)
#     assert (cmd.arg == [])
#     assert (cmd.kwarg == {})
#
#
# class Test:
#     def main(self):
#         test_command_class()
#         # test_ping = AsyncCommand(fun='test.ping', prev_cmd=ConcreteCommand())
#         # test_ping.execute()
#         # print(test_ping.__repr__())
#
# if __name__ == '__main__':
#     Test().main()