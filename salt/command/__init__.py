'''
Contains the Command class and CommandDecorator for now
Contains code for ValidatingDecorator
'''


class Command(object):
    '''
    The command base class
    '''

    FUN = 'test.ping'
    WHEEL = 'wheel'
    RUNNER = 'runner'
    LOCAL = 'local'
    CLIENT = LOCAL

    def __init__(self):
        '''
        :param prev_cmd: Type Command.
         The command decorator.
        '''
        self.client = self.CLIENT
        self.fun = self.FUN
        self.arg = []
        self.kwarg = {}
        self.tgt = None
        self.tgt_type = None

    def command_to_execute(self):
        '''
        :return: None. Meant to be overridden by subclasses
        '''
        raise NotImplementedError(
            'Command base class does not implement an execute method')

    def __repr__(self):
        return ' client = {0},' \
               ' fun = {1}' \
               ' arg = {2}' \
               ' kwarg = {3}' \
               ' tgt = {4}' \
               ' tgt_type = {5}'\
            .format(self.client,
                    self.fun,
                    self.arg,
                    self.kwarg,
                    self.tgt,
                    self.tgt_type)


class MinionCommand(Command):
    '''
    The concrete implementation
    '''
    CLIENT = [('client', 'local')]

    def command_to_execute(self):
        return dict(self.CLIENT)
