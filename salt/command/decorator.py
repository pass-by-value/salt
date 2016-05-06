'''
Decorators module, defines

1. The base class CommandDecorator that inherits from command
2. Additional decorators
'''

from salt.command import Command


class CommandDecorator(Command):
    '''
    The decorator base class, inherits from Command
    '''

    def command_to_execute(self):
        return {}


class FunctionDecorator(CommandDecorator):
    '''
    Adds client information
    '''
    FUNCTION = [('fun', 'test.ping')]

    def __init__(self, fun='', decorated=None):
        super(FunctionDecorator, self).__init__()
        self.decorated = decorated
        if fun:
            self.fun = [('fun', fun)]
        else:
            self.fun = self.FUNCTION

    def command_to_execute(self):
        ret = dict(self.fun)

        if self.decorated:
            ret.update(self.decorated.command_to_execute())

        return ret


class TgtDecorator(CommandDecorator):
    '''
    Adds tgt information
    '''
    TGT = [('tgt', '')]

    def __init__(self, tgt='', decorated=None):
        super(TgtDecorator, self).__init__()
        self.decorated = decorated
        if tgt:
            self.tgt = {'tgt': tgt}
        else:
            self.tgt = dict(self.TGT)

    def command_to_execute(self):
        ret = self.tgt

        if self.decorated:
            ret.update(self.decorated.command_to_execute())

        return ret


class TgtTypeDecorator(CommandDecorator):
    '''
    Adds tgt information
    '''
    TGT_TYPE = [('expr_form', '')]

    def __init__(self, tgt_type='', decorated=None):
        super(TgtTypeDecorator, self).__init__()
        self.decorated = decorated
        if tgt_type:
            self.tgt_type = {'expr_form': tgt_type}
        else:
            self.tgt_type = dict(self.TGT_TYPE)

    def command_to_execute(self):
        ret = self.tgt_type

        if self.decorated:
            ret.update(self.decorated.command_to_execute())

        return ret
