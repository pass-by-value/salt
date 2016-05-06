import unittest

from salt.command import Command, MinionCommand
from salt.command.decorator import CommandDecorator,\
    FunctionDecorator,\
    TgtDecorator,\
    TgtTypeDecorator


class CommandTest(unittest.TestCase):

    def setUp(self):
        self.cmd = Command()

    def test_command_instance_creation_works(self):
        self.assertIsNone(self.cmd.tgt, 'command class tgt init error')
        self.assertIsNone(self.cmd.tgt_type, 'command class tgt type init error')
        self.assertEqual(self.cmd.fun, Command.FUN, 'command class fun init error')
        self.assertEqual(self.cmd.client, Command.CLIENT, 'command class client init error')
        self.assertEqual(self.cmd.arg, [], 'command class arg init error')
        self.assertEqual(self.cmd.kwarg, {}, 'command class kwarg init error')

    def test_execute_throws_error(self):
        self.assertRaises(NotImplementedError)


class DecoratorTests(unittest.TestCase):
    def test_execute_method_works(self):
        decorator = CommandDecorator()
        self.assertEqual(decorator.command_to_execute(), {})


class FunctionDecoratorTests(unittest.TestCase):
    def test_execute_method_works(self):
        command = MinionCommand()
        command_with_client = FunctionDecorator(decorated=command)

        self.assertEquals(
            command_with_client.command_to_execute(),
            {'fun': 'test.ping',
             'client': 'local'}
        )


class TgtDecoratorTests(unittest.TestCase):
    def test_execute_method_works(self):
        command_with_tgt = TgtDecorator(tgt='foo')

        self.assertEquals(
            command_with_tgt.command_to_execute(),
            {'tgt': 'foo'}
        )


class TgtTypeDecoratorTests(unittest.TestCase):
    def test_execute_method_works(self):
        command_with_client = TgtTypeDecorator(tgt_type='glob')

        self.assertEquals(
            command_with_client.command_to_execute(),
            {'expr_form': 'glob'}
        )


class DecoratedCommandTests(unittest.TestCase):
    def test_allows_building_functions(self):
        base_cmd = MinionCommand()
        minion_cmd_with_function = FunctionDecorator(fun='disk.usage',
                                                     decorated=base_cmd)
        self.assertEqual(minion_cmd_with_function.command_to_execute(),
                         {'fun': 'disk.usage', 'client': 'local'})

if __name__ == '__main__':
    unittest.main()
